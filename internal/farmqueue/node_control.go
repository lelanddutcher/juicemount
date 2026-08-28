package farmqueue

// Operational worker lifecycle control.
//
// Worker configuration and worker lifecycle are deliberately separate. Config
// is durable desired state (quality, concurrency, device); restart is a
// one-shot command and disable is a durable admission gate. Keeping lifecycle
// values out of FarmConfig prevents a replayed config revision from restarting
// a container forever.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	WorkerCommandPrefix       = "juicefarm:worker-command:"
	WorkerCommandStatusPrefix = "juicefarm:worker-command-status:"
	WorkerDisabledPrefix      = "juicefarm:worker-disabled:"
	workerControlTTL          = 7 * 24 * time.Hour
	WorkerActionRestart       = "restart"
)

// WorkerCommand is a one-shot instruction consumed by exactly one live
// process advertising Name. The random runtime worker ID is intentionally not
// the target: it changes on every container restart, while Name is the stable
// Manager identity and config key.
type WorkerCommand struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Action      string `json:"action"`
	RequestedAt string `json:"requested_at"`
	RequestedBy string `json:"requested_by,omitempty"`
}

// WorkerCommandStatus is the small audit/ack record the Manager can show while
// a restart is in flight. It contains no credentials or command output.
type WorkerCommandStatus struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Action         string `json:"action"`
	State          string `json:"state"` // requested|acknowledged
	RequestedAt    string `json:"requested_at"`
	RequestedBy    string `json:"requested_by,omitempty"`
	AcknowledgedAt string `json:"acknowledged_at,omitempty"`
	WorkerID       string `json:"worker_id,omitempty"`
}

// ValidWorkerControlName accepts the same host-style stable names as Manager.
// A strict alphabet also keeps each Redis key a single unsurprising namespace
// component when a worker is configured manually.
func ValidWorkerControlName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

func validateWorkerControlName(name string) (string, error) {
	name = strings.TrimSpace(strings.ToLower(name))
	if !ValidWorkerControlName(name) {
		return "", fmt.Errorf("invalid stable worker name %q", name)
	}
	return name, nil
}

// RequestWorkerRestart publishes a one-shot restart request. A worker
// acknowledges the command before cancelling its current job; the durable
// claim is then requeued and the container's unless-stopped policy starts the
// exact same artifact again.
func (c *Client) RequestWorkerRestart(ctx context.Context, name, requestedBy string) (WorkerCommand, error) {
	name, err := validateWorkerControlName(name)
	if err != nil {
		return WorkerCommand{}, err
	}
	cmd := WorkerCommand{
		ID:          NewID(),
		Name:        name,
		Action:      WorkerActionRestart,
		RequestedAt: nowISO(),
		RequestedBy: strings.TrimSpace(requestedBy),
	}
	raw, err := json.Marshal(cmd)
	if err != nil {
		return WorkerCommand{}, err
	}
	status := map[string]any{
		"id": cmd.ID, "name": cmd.Name, "action": cmd.Action,
		"state": "requested", "requested_at": cmd.RequestedAt,
		"requested_by": cmd.RequestedBy,
	}
	_, err = c.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.LPush(ctx, WorkerCommandPrefix+name, raw)
		p.Expire(ctx, WorkerCommandPrefix+name, workerControlTTL)
		p.HSet(ctx, WorkerCommandStatusPrefix+cmd.ID, status)
		p.Expire(ctx, WorkerCommandStatusPrefix+cmd.ID, workerControlTTL)
		return nil
	})
	return cmd, err
}

// WaitWorkerCommand returns one lifecycle command for name. timeout <= 0 is a
// non-blocking pop, which keeps unit tests and shutdown paths deterministic.
func (c *Client) WaitWorkerCommand(ctx context.Context, name string, timeout time.Duration) (WorkerCommand, bool, error) {
	name, err := validateWorkerControlName(name)
	if err != nil {
		return WorkerCommand{}, false, err
	}
	var raw string
	if timeout <= 0 {
		raw, err = c.rdb.RPop(ctx, WorkerCommandPrefix+name).Result()
		if err == redis.Nil {
			return WorkerCommand{}, false, nil
		}
	} else {
		var values []string
		values, err = c.rdb.BRPop(ctx, timeout, WorkerCommandPrefix+name).Result()
		if err == redis.Nil {
			return WorkerCommand{}, false, nil
		}
		if err == nil && len(values) == 2 {
			raw = values[1]
		}
	}
	if err != nil {
		return WorkerCommand{}, false, err
	}
	var cmd WorkerCommand
	if err := json.Unmarshal([]byte(raw), &cmd); err != nil {
		return WorkerCommand{}, false, fmt.Errorf("decode worker command: %w", err)
	}
	if cmd.Name != name || cmd.Action != WorkerActionRestart || cmd.ID == "" {
		return WorkerCommand{}, false, fmt.Errorf("invalid worker command for %q", name)
	}
	return cmd, true, nil
}

// AcknowledgeWorkerCommand records that a live worker consumed the request.
// The worker does this before it interrupts media work so the UI never claims a
// restart was accepted merely because an item disappeared from a Redis list.
func (c *Client) AcknowledgeWorkerCommand(ctx context.Context, cmd WorkerCommand, workerID string) error {
	if cmd.ID == "" || cmd.Name == "" || cmd.Action != WorkerActionRestart {
		return fmt.Errorf("invalid worker command acknowledgement")
	}
	key := WorkerCommandStatusPrefix + cmd.ID
	_, err := c.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, key, map[string]any{
			"state": "acknowledged", "acknowledged_at": nowISO(), "worker_id": workerID,
		})
		p.Expire(ctx, key, workerControlTTL)
		return nil
	})
	return err
}

// WorkerCommandStatusByID reads a restart audit record.
func (c *Client) WorkerCommandStatusByID(ctx context.Context, id string) (WorkerCommandStatus, bool, error) {
	if id == "" {
		return WorkerCommandStatus{}, false, nil
	}
	m, err := c.rdb.HGetAll(ctx, WorkerCommandStatusPrefix+id).Result()
	if err != nil {
		return WorkerCommandStatus{}, false, err
	}
	if len(m) == 0 {
		return WorkerCommandStatus{}, false, nil
	}
	return WorkerCommandStatus{
		ID: m["id"], Name: m["name"], Action: m["action"], State: m["state"],
		RequestedAt: m["requested_at"], RequestedBy: m["requested_by"],
		AcknowledgedAt: m["acknowledged_at"], WorkerID: m["worker_id"],
	}, true, nil
}

// SetWorkerDisabled is the durable admission gate. Disabled workers continue
// to heartbeat with state=disabled so the Manager distinguishes an intentional
// drain from an outage; they cannot claim new jobs. Clearing the key resumes
// the same verified process without requiring a container restart.
func (c *Client) SetWorkerDisabled(ctx context.Context, name string, disabled bool) error {
	name, err := validateWorkerControlName(name)
	if err != nil {
		return err
	}
	key := WorkerDisabledPrefix + name
	if !disabled {
		return c.rdb.Del(ctx, key).Err()
	}
	return c.rdb.Set(ctx, key, nowISO(), 0).Err()
}

func (c *Client) WorkerDisabled(ctx context.Context, name string) (bool, error) {
	name, err := validateWorkerControlName(name)
	if err != nil {
		return false, err
	}
	n, err := c.rdb.Exists(ctx, WorkerDisabledPrefix+name).Result()
	return n > 0, err
}

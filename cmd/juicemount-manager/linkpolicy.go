package main

// Service exposure and the Headscale ACL policy (Link stages 1-2).
//
// The NAS node serves exactly the JuiceMount ports on its own tailnet
// address and forwards each to its compose service. Each service keeps the
// port the NAS host publishes on the LAN, so a Mac reaches the same port on
// the NAS node's tailnet address that it reaches on the NAS's LAN address.
// The policy lets linked Macs and farm nodes reach those ports on the NAS
// node and nothing else: not the router, not other LAN hosts, not each other.

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

// defaultNASServices maps each published JuiceMount port to the service
// behind it: the RESP proxy (Redis), MinIO's S3 endpoint, and this Manager.
// Override with JM_NET_NAS_SERVICES ("port=host:port,..."; a target of
// "manager" serves this Manager's handler in-process).
const defaultNASServices = "30179=respproxy:6379,30151=minio:9000,30190=" + nasManagerTarget

// nasManagerTarget names the Manager itself as a service target.
const nasManagerTarget = "manager"

type nasService struct {
	Port   uint16 // tailnet listen port on the NAS node
	Target string // host:port dialed on the container network, or nasManagerTarget
}

func nasServices() ([]nasService, error) {
	return parseNASServices(envOr("JM_NET_NAS_SERVICES", defaultNASServices))
}

func parseNASServices(raw string) ([]nasService, error) {
	var out []nasService
	seen := map[uint16]bool{}
	for _, item := range splitNonEmpty(raw, ",") {
		portRaw, target, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("JM_NET_NAS_SERVICES entry %q must be port=host:port", item)
		}
		port, err := strconv.ParseUint(strings.TrimSpace(portRaw), 10, 16)
		if err != nil || port == 0 {
			return nil, fmt.Errorf("JM_NET_NAS_SERVICES entry %q has an invalid port", item)
		}
		target = strings.TrimSpace(target)
		if target != nasManagerTarget {
			host, targetPort, err := net.SplitHostPort(target)
			if err != nil || host == "" || targetPort == "" {
				return nil, fmt.Errorf("JM_NET_NAS_SERVICES entry %q needs a host:port target or %q", item, nasManagerTarget)
			}
		}
		if seen[uint16(port)] {
			return nil, fmt.Errorf("JM_NET_NAS_SERVICES lists port %d twice", port)
		}
		seen[uint16(port)] = true
		out = append(out, nasService{Port: uint16(port), Target: target})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("JM_NET_NAS_SERVICES lists no services")
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Port < out[j].Port })
	return out, nil
}

// linkPolicy renders the Headscale (policy v2) ACL. Sources are tagged Macs
// and farm nodes plus untagged nodes of the Link user, which covers Macs
// paired before keys were tagged. The only destinations are the service
// ports on tag:jm-nas and, in subnet exposure, the same ports inside that
// route: on nasAddr (the NAS's own LAN address as a host prefix) when it is
// known, else on every address in it.
//
// No tag has an owner. Headscale 0.29 still applies the tags of a key the
// Manager mints and lets `headscale nodes tag` set a tag the policy defines,
// but no node may request a tag itself: with jm@ as owner, any untagged
// Link-user node (a Mac paired before tagged keys) could advertise
// tag:jm-nas and receive the traffic Macs send to the NAS.
func linkPolicy(user string, services []nasService, subnet, nasAddr string) ([]byte, error) {
	if user == "" || len(services) == 0 {
		return nil, fmt.Errorf("link policy needs a user and at least one service")
	}
	ports := make([]string, 0, len(services))
	for _, svc := range services {
		ports = append(ports, strconv.Itoa(int(svc.Port)))
	}
	portList := strings.Join(ports, ",")
	dst := []string{manager.LinkTagNAS + ":" + portList}
	if subnet != "" {
		prefix, err := netip.ParsePrefix(subnet)
		if err != nil {
			return nil, fmt.Errorf("bad JM_NET_NAS_ROUTE %q: %w", subnet, err)
		}
		target := prefix.Masked()
		if nasAddr != "" {
			addr, err := netip.ParsePrefix(nasAddr)
			if err != nil || !addr.IsSingleIP() || !prefix.Contains(addr.Addr()) {
				return nil, fmt.Errorf("NAS address %q is not one address inside %s", nasAddr, prefix)
			}
			target = addr
		}
		dst = append(dst, target.String()+":"+portList)
	}
	owner := []string{} // defined, unowned: JSON [] (not null)
	type acl struct {
		Action string   `json:"action"`
		Src    []string `json:"src"`
		Dst    []string `json:"dst"`
	}
	policy := struct {
		TagOwners map[string][]string `json:"tagOwners"`
		ACLs      []acl               `json:"acls"`
	}{
		TagOwners: map[string][]string{
			manager.LinkTagMac:  owner,
			manager.LinkTagNAS:  owner,
			manager.LinkTagFarm: owner,
		},
		ACLs: []acl{{
			Action: "accept",
			Src:    []string{manager.LinkTagMac, manager.LinkTagFarm, user + "@"},
			Dst:    dst,
		}},
	}
	return json.MarshalIndent(policy, "", "  ")
}

// writeLinkPolicy regenerates the Manager-owned policy file next to
// Headscale's config.yaml. Unlike config.yaml it is not user-owned: it
// follows the service map and exposure on every start.
//
// A misconfigured exposure stops only the NAS node (nasNodeConfigFromEnv
// fails with the same error, shown by GET /api/net/link); the control
// server keeps running with a policy that reaches tag:jm-nas only, which no
// running node then holds.
func writeLinkPolicy(cfgPath string) (string, error) {
	services, err := nasServices()
	if err != nil {
		return "", err
	}
	subnet, nasAddr := "", ""
	if _, s, err := manager.LinkExposure(cfgPath); err != nil {
		log.Printf("JuiceMount Link: NAS exposure is misconfigured (%v); the ACL policy reaches no subnet route until it is fixed", err)
	} else if subnet = s; subnet != "" {
		if nasAddr, err = manager.LinkSubnetNASAddr(subnet); err != nil {
			return "", err
		}
	}
	body, err := linkPolicy(manager.LinkUserName(), services, subnet, nasAddr)
	if err != nil {
		return "", err
	}
	path := filepath.Join(filepath.Dir(cfgPath), "policy.hujson")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(body, '\n'), 0o600); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", err
	}
	return path, nil
}

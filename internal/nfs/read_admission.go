package nfs

import (
	"context"
	"time"
)

// rejectSaturatedRead runs on the connection reader, without a new goroutine.
// The existing finite response queue supplies egress backpressure if the client
// stops reading. It never waits for backend work or a data-read admission slot.
func (c *conn) rejectSaturatedRead(ctx context.Context, w *response) error {
	c.Server.rpcStarted()
	defer c.Server.rpcDone()
	start := time.Now()
	w.errorFmt = opAttrErrorFormatter // READ3resfail includes post_op_attr.
	if err := w.drain(ctx); err != nil {
		putResponseBuffer(w.writer)
		return err
	}
	if err := c.err(ctx, w, &NFSStatusError{NFSStatusJukebox, nil}); err != nil {
		putResponseBuffer(w.writer)
		return err
	}
	recordJukebox(inflightOpName(w.req))
	err := w.finish(ctx)
	rpcCount.Add(1)
	if obs := currentObserver(); obs != nil {
		obs(w.req.Header.Prog, w.req.Header.Proc, time.Since(start), err)
	}
	return err
}

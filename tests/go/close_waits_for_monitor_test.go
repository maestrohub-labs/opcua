//go:build integration
// +build integration

package uatest2

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestrohub-labs/opcua"
	"github.com/maestrohub-labs/opcua/ua"
	"github.com/stretchr/testify/require"
)

// TestCloseWaitsForTheReconnectMonitor: Close cancels the reconnect
// monitor and the publish loop, and returns only once both have ended.
//
// Close used to cancel them and return at once. A monitor caught in the
// middle of a step (a restore, a handshake, a callback of the caller's)
// went on after Close had returned, and could still call back into the
// caller — OnSessionAbandoned handing over the session its cancelled
// restore gave up, StateChangedFunc — when the caller had already torn
// its own side down. Here the monitor is held for a moment inside the
// caller's StateChangedFunc when Close is called: Close must wait for it.
func TestCloseWaitsForTheReconnectMonitor(t *testing.T) {
	ctx := context.Background()

	srv := startServer()
	defer srv.Close()
	time.Sleep(2 * time.Second)

	proxy := newCutProxy(t, "localhost:4840")

	inMonitor := make(chan struct{})
	var held atomic.Bool
	c, err := opcua.NewClient("opc.tcp://"+proxy.addr(),
		opcua.SecurityMode(ua.MessageSecurityModeNone),
		opcua.AutoReconnect(true),
		opcua.ReconnectInterval(200*time.Millisecond),
		opcua.StateChangedFunc(func(s opcua.ConnState) {
			// Set by the monitor when the link drops: hold it there once.
			if s == opcua.Disconnected && held.CompareAndSwap(false, true) {
				close(inMonitor)
				time.Sleep(300 * time.Millisecond)
			}
		}),
	)
	require.NoError(t, err)
	require.NoError(t, c.Connect(ctx))

	proxy.cutAll()
	select {
	case <-inMonitor:
	case <-time.After(10 * time.Second):
		t.Fatal("the monitor never reported the dropped link")
	}

	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, c.Close(closeCtx))

	if alive := clientLoops(); alive != "" {
		t.Fatalf("Close returned while the client's loops still ran:\n%s", alive)
	}
}

// clientLoops returns the stacks of goroutines running the client's
// reconnect monitor or publish loop, or "".
func clientLoops() string {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "opcua.(*Client).monitor(") || strings.Contains(g, "opcua.(*Client).monitorSubscriptions(") {
			out = append(out, g)
		}
	}
	return strings.Join(out, "\n\n")
}

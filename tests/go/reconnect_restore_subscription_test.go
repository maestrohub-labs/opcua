//go:build integration
// +build integration

package uatest2

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestrohub-labs/opcua"
	"github.com/maestrohub-labs/opcua/server"
	"github.com/maestrohub-labs/opcua/server/attrs"
	"github.com/maestrohub-labs/opcua/ua"
	"github.com/stretchr/testify/require"
)

// TestRestoreSessionResumesSubscriptions: the link drops and comes
// straight back, and the monitor reactivates the SAME session on the new
// secure channel (restoreSession). The session's subscriptions survived on
// the server, but the publish loop was paused when the link dropped — it
// must be resumed, or data stops for good while every request still works.
//
// Before the fix the restore path went to restoreSubscriptions with
// nothing listed to republish or recreate, so activeSubs was 0 and the
// publish loop was never resumed: no PublishRequest after the blip,
// against every server tried (OPC Foundation .NET reference server,
// opc-plc, open62541, and this one).
func TestRestoreSessionResumesSubscriptions(t *testing.T) {
	ctx := context.Background()

	srv := startServer()
	defer srv.Close()
	time.Sleep(2 * time.Second)

	proxy := newCutProxy(t, "localhost:4840")

	c, err := opcua.NewClient("opc.tcp://"+proxy.addr(),
		opcua.SecurityMode(ua.MessageSecurityModeNone),
		opcua.AutoReconnect(true),
		opcua.ReconnectInterval(200*time.Millisecond),
	)
	require.NoError(t, err)
	require.NoError(t, c.Connect(ctx))
	defer c.Close(ctx)

	// A node whose value changes on every tick. Driven by a value func and
	// ChangeNotification, not by Write: gopcua's server races a Write's
	// SetAttribute against the monitored item reading the same attribute
	// (server/node.go), which -race reports and which is not under test.
	ns, err := srv.Namespace(1)
	require.NoError(t, err)
	nodeNS, ok := ns.(*server.NodeNameSpace)
	require.True(t, ok)
	nodeID := ua.NewStringNodeID(nodeNS.ID(), "restore_counter")
	var counter atomic.Int32
	nodeNS.AddNode(server.NewNode(nodeID,
		map[ua.AttributeID]*ua.DataValue{
			ua.AttributeIDBrowseName: server.DataValueFromValue(attrs.BrowseName("restore_counter")),
			ua.AttributeIDNodeClass:  server.DataValueFromValue(uint32(ua.NodeClassVariable)),
		},
		nil,
		func() *ua.DataValue { return server.DataValueFromValue(counter.Load()) },
	))
	stopTicker := make(chan struct{})
	var tickerDone sync.WaitGroup
	tickerDone.Add(1)
	go func() {
		defer tickerDone.Done()
		for {
			select {
			case <-stopTicker:
				return
			case <-time.After(100 * time.Millisecond):
			}
			counter.Add(1)
			nodeNS.ChangeNotification(nodeID)
		}
	}()
	defer func() { close(stopTicker); tickerDone.Wait() }()

	notifyCh := make(chan *opcua.PublishNotificationData, 64)
	sub, err := c.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 100 * time.Millisecond}, notifyCh)
	require.NoError(t, err)
	_, err = sub.Monitor(ctx, ua.TimestampsToReturnBoth, opcua.NewMonitoredItemCreateRequestWithDefaults(nodeID, ua.AttributeIDValue, 1))
	require.NoError(t, err)

	var received atomic.Int64
	go func() {
		for n := range notifyCh {
			if n.Error == nil {
				received.Add(1)
			}
		}
	}()
	require.Eventually(t, func() bool { return received.Load() >= 5 }, 10*time.Second, 50*time.Millisecond, "data flows before the blip")

	proxy.cutAll()
	require.Eventually(t, func() bool { return c.State() == opcua.Connected && proxy.links() > 0 }, 10*time.Second, 50*time.Millisecond)
	_, err = c.Read(ctx, &ua.ReadRequest{NodesToRead: []*ua.ReadValueID{{NodeID: nodeID, AttributeID: ua.AttributeIDValue}}})
	require.NoError(t, err, "the restored session answers requests")

	afterBlip := received.Load()
	require.Eventually(t, func() bool { return received.Load() >= afterBlip+5 }, 10*time.Second, 50*time.Millisecond,
		"the subscription must deliver again after the session was restored in place")
}

// cutProxy is a TCP proxy whose links the test can drop.
type cutProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	conns  []net.Conn
}

func newCutProxy(t *testing.T, target string) *cutProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &cutProxy{ln: ln, target: target}
	t.Cleanup(func() { _ = ln.Close(); p.cutAll() })
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			out, err := net.Dial("tcp", target)
			if err != nil {
				_ = in.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, in, out)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(out, in); _ = out.Close() }()
			go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		}
	}()
	return p
}

func (p *cutProxy) addr() string { return p.ln.Addr().String() }

func (p *cutProxy) links() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns) / 2
}

func (p *cutProxy) cutAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

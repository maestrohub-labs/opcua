//go:build integration
// +build integration

package uatest2

import (
	"context"
	"testing"
	"time"

	"github.com/maestrohub-labs/opcua"
	"github.com/maestrohub-labs/opcua/server"
	"github.com/maestrohub-labs/opcua/ua"
	"github.com/stretchr/testify/require"
)

// TestResumeTakesOverTheSessionAndItsSubscriptions: a client whose channel
// is gone leaves its session on the server; a second client resumes it
// over a new channel and takes the first one's subscription as it is (the
// same id: none is created again), whose notifications then reach the same
// channel over the new secure channel, numbered on.
func TestResumeTakesOverTheSessionAndItsSubscriptions(t *testing.T) {
	ctx := context.Background()
	srv := server.New(server.EndPoint("127.0.0.1", 0),
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous))
	ns := server.NewNodeNameSpace(srv, "resume")
	node := ns.AddNewVariableStringNode("counter", int32(0))
	ns.Objects().AddVariable(node)
	require.NoError(t, srv.Start(ctx))
	defer srv.Close()
	url := srv.URLs()[0]
	opts := []opcua.Option{opcua.SecurityMode(ua.MessageSecurityModeNone), opcua.AutoReconnect(false)}

	first, err := opcua.NewClient(url, opts...)
	require.NoError(t, err)
	require.NoError(t, first.Connect(ctx))
	notifs := make(chan *opcua.PublishNotificationData, 64)
	sub, err := first.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 50 * time.Millisecond}, notifs)
	require.NoError(t, err)
	_, err = sub.Monitor(ctx, ua.TimestampsToReturnBoth, opcua.NewMonitoredItemCreateRequestWithDefaults(node.ID(), ua.AttributeIDValue, 1))
	require.NoError(t, err)
	var last uint32
	select {
	case n := <-notifs:
		require.NoError(t, n.Error)
		last = n.SequenceNumber
	case <-time.After(5 * time.Second):
		t.Fatal("no first notification")
	}

	// The first client goes away without closing its session.
	session, err := first.DetachSession(ctx)
	require.NoError(t, err)
	require.NoError(t, first.Close(ctx))

	second, err := opcua.NewClient(url, opts...)
	require.NoError(t, err)
	adopted, err := second.Resume(ctx, session, []*opcua.Subscription{sub})
	require.NoError(t, err)
	defer second.Close(ctx)
	require.Len(t, adopted, 1)
	require.Equal(t, sub.SubscriptionID, adopted[0].SubscriptionID, "the same subscription, not a new one")
	require.Same(t, session, second.Session())

	require.Contains(t, second.SubscriptionIDs(), sub.SubscriptionID, "registered with the resuming client")

	// The resumed subscription publishes over the new channel, numbered on
	// from where the first client left it. The value changes until a
	// change arrives: this server may answer the first client's last
	// Publish, queued before its channel went, into that dead channel, and
	// keeps no message to republish (a real stack does).
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(10 * time.Second)
	for v := int32(42); ; {
		select {
		case <-tick.C:
			node.SetAttribute(ua.AttributeIDValue, server.DataValueFromValue(v))
			srv.ChangeNotification(node.ID())
			v++
		case n := <-notifs:
			require.NoError(t, n.Error)
			require.Greater(t, n.SequenceNumber, last, "numbered on from the first client's")
			if dc, ok := n.Value.(*ua.DataChangeNotification); ok && len(dc.MonitoredItems) > 0 {
				if x, _ := dc.MonitoredItems[0].Value.Value.Value().(int32); x >= 42 {
					return
				}
			}
		case <-deadline:
			t.Fatal("the resumed subscription delivered nothing over the new channel")
		}
	}
}

// TestResumeOfASessionTheServerNoLongerHas fails and leaves the client
// unconnected.
func TestResumeOfASessionTheServerNoLongerHas(t *testing.T) {
	ctx := context.Background()
	srv := server.New(server.EndPoint("127.0.0.1", 0),
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous))
	require.NoError(t, srv.Start(ctx))
	defer srv.Close()
	opts := []opcua.Option{opcua.SecurityMode(ua.MessageSecurityModeNone), opcua.AutoReconnect(false)}

	first, err := opcua.NewClient(srv.URLs()[0], opts...)
	require.NoError(t, err)
	require.NoError(t, first.Connect(ctx))
	session := first.Session()
	require.NoError(t, first.Close(ctx)) // closes the session on the server

	second, err := opcua.NewClient(srv.URLs()[0], opts...)
	require.NoError(t, err)
	_, err = second.Resume(ctx, session, nil)
	require.Error(t, err)
	require.Nil(t, second.SecureChannel())
	require.Nil(t, second.Session())
}

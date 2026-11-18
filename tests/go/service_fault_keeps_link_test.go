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

// TestServiceFaultKeepsTheLink: a response with a Bad service result (a
// ServiceFault for a service the server does not offer) is that request's
// answer, not a failure of the secure channel. A client that does not
// reconnect by itself must stay Connected and go on answering.
//
// The channel used to put the status on the client's error channel, where
// the monitor took it for a lost link: Disconnected, then Closed for good,
// after one refused request.
func TestServiceFaultKeepsTheLink(t *testing.T) {
	ctx := context.Background()
	srv := server.New(server.EndPoint("127.0.0.1", 0),
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous))
	require.NoError(t, srv.Start(ctx))
	defer srv.Close()

	states := make(chan opcua.ConnState, 16)
	c, err := opcua.NewClient(srv.URLs()[0], opcua.SecurityMode(ua.MessageSecurityModeNone), opcua.AutoReconnect(false),
		opcua.StateChangedFunc(func(s opcua.ConnState) { states <- s }))
	require.NoError(t, err)
	require.NoError(t, c.Connect(ctx))
	defer c.Close(ctx)

	err = c.Send(ctx, &ua.TransferSubscriptionsRequest{SubscriptionIDs: []uint32{1}}, func(ua.Response) error { return nil })
	require.ErrorIs(t, err, ua.StatusBadServiceUnsupported, "the request gets the server's answer")

	time.Sleep(300 * time.Millisecond) // the monitor would have acted by now
	require.Equal(t, opcua.Connected, c.State())
	for len(states) > 0 {
		require.NotEqual(t, opcua.Disconnected, <-states, "no lost link was reported")
	}
	_, err = c.Node(ua.NewNumericNodeID(0, 2258)).Value(ctx)
	require.NoError(t, err, "the client still answers")
}

// TestAClosedSessionIsALostLink: the server closes the client's session
// while its secure channel stays up. Its next request answers
// Bad_SessionIdInvalid: unlike other Bad service results, that is a lost
// link (nothing more can be done on the session), reported at once.
func TestAClosedSessionIsALostLink(t *testing.T) {
	ctx := context.Background()
	srv := server.New(server.EndPoint("127.0.0.1", 0),
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous))
	require.NoError(t, srv.Start(ctx))
	defer srv.Close()
	// A short request timeout: the Publish outstanding when the session
	// closes is never answered here; the next one, sent once it times out,
	// meets the closed session.
	opts := []opcua.Option{opcua.SecurityMode(ua.MessageSecurityModeNone), opcua.AutoReconnect(false), opcua.RequestTimeout(time.Second)}

	states := make(chan opcua.ConnState, 16)
	c, err := opcua.NewClient(srv.URLs()[0], append(opts, opcua.StateChangedFunc(func(s opcua.ConnState) { states <- s }))...)
	require.NoError(t, err)
	require.NoError(t, c.Connect(ctx))
	defer c.Close(ctx)
	// A subscription: its Publish requests are what meet the closed session.
	_, err = c.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 50 * time.Millisecond, MaxKeepAliveCount: 2, LifetimeCount: 60}, make(chan *opcua.PublishNotificationData, 16))
	require.NoError(t, err)

	// Another client takes the session over and closes it on the server.
	other, err := opcua.NewClient(srv.URLs()[0], opts...)
	require.NoError(t, err)
	require.NoError(t, other.Dial(ctx))
	require.NoError(t, other.ActivateSession(ctx, c.Session()))
	require.NoError(t, other.CloseSession(ctx))
	_ = other.Close(ctx)

	deadline := time.After(10 * time.Second)
	for {
		select {
		case s := <-states:
			if s == opcua.Disconnected {
				return
			}
		case <-deadline:
			t.Fatalf("no lost link was reported for a closed session (state %v)", c.State())
		}
	}
}

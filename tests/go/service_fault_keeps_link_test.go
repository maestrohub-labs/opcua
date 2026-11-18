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

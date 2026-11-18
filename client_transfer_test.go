package opcua

import (
	"context"
	"testing"

	"github.com/maestrohub-labs/opcua/ua"
	"github.com/stretchr/testify/require"
)

// oldSubscription is a subscription of another (closed) client, as
// TransferSubscriptions is given it.
func oldSubscription(id uint32, notifs chan *PublishNotificationData, lastSeq uint32) *Subscription {
	return &Subscription{
		SubscriptionID:            id,
		RevisedPublishingInterval: 250e6,
		RevisedLifetimeCount:      100,
		RevisedMaxKeepAliveCount:  10,
		Notifs:                    notifs,
		params:                    &SubscriptionParameters{Interval: 250e6},
		items: map[uint32]*monitoredItem{1: {
			req: NewMonitoredItemCreateRequestWithDefaults(ua.NewNumericNodeID(0, 2258), ua.AttributeIDValue, 1),
		}},
		lastSeq: lastSeq,
		nextSeq: lastSeq + 1,
		c:       &stubClient{},
	}
}

// The subscriptions are this client's before the request goes out; the
// ones the server transferred stay, with the old Notifs and sequence
// position; the refused one is forgotten; the available sequence numbers
// are the server's.
func TestTransferSubscriptions_RegistersFirstAndKeepsOnlyTheTransferred(t *testing.T) {
	c, err := NewClient("opc.tcp://example.com:4840")
	require.NoError(t, err)
	ch1, ch2 := make(chan *PublishNotificationData, 1), make(chan *PublishNotificationData, 1)
	subs := []*Subscription{oldSubscription(7, ch1, 41), oldSubscription(8, ch2, 3)}

	send := func(_ context.Context, req ua.Request, h func(ua.Response) error) error {
		r := req.(*ua.TransferSubscriptionsRequest)
		require.Equal(t, []uint32{7, 8}, r.SubscriptionIDs)
		require.False(t, r.SendInitialValues)
		c.subMux.RLock()
		_, has7 := c.subs[7]
		_, has8 := c.subs[8]
		c.subMux.RUnlock()
		require.True(t, has7 && has8, "registered before the server can publish for them")
		return h(&ua.TransferSubscriptionsResponse{
			ResponseHeader: &ua.ResponseHeader{},
			Results: []*ua.TransferResult{
				{StatusCode: ua.StatusOK, AvailableSequenceNumbers: []uint32{42, 43}},
				{StatusCode: ua.StatusBadSubscriptionIDInvalid},
			},
		})
	}
	gen := c.subsGen.Load()
	out, err := c.transferAndAdopt(context.Background(), subs, send)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.NotEqual(t, gen, c.subsGen.Load(), "a registration the publish loop sees")

	require.NotNil(t, out[0].Subscription)
	require.Equal(t, ua.StatusOK, out[0].Status)
	require.Equal(t, []uint32{42, 43}, out[0].AvailableSequenceNumbers)
	got := out[0].Subscription
	require.Same(t, c, got.c.(*Client))
	require.Equal(t, uint32(7), got.SubscriptionID)
	require.Equal(t, uint32(41), got.lastSeq)
	require.Equal(t, uint32(42), got.nextSeq)
	require.Len(t, got.items, 1)
	got.notify(context.Background(), &PublishNotificationData{SubscriptionID: 7})
	require.Len(t, ch1, 1, "the old subscription's Notifs")

	require.Nil(t, out[1].Subscription)
	require.Equal(t, ua.StatusBadSubscriptionIDInvalid, out[1].Status)
	c.subMux.RLock()
	defer c.subMux.RUnlock()
	require.Same(t, got, c.subs[7])
	require.NotContains(t, c.subs, uint32(8), "the refused one is forgotten")
}

// A refusal of the whole request (Bad_ServiceUnsupported) or a transport
// error forgets every subscription it registered.
func TestTransferSubscriptions_ServiceRefusedForgetsAll(t *testing.T) {
	for name, send := range map[string]func(context.Context, ua.Request, func(ua.Response) error) error{
		"service result": func(_ context.Context, _ ua.Request, h func(ua.Response) error) error {
			return h(&ua.TransferSubscriptionsResponse{ResponseHeader: &ua.ResponseHeader{ServiceResult: ua.StatusBadServiceUnsupported}})
		},
		"error": func(context.Context, ua.Request, func(ua.Response) error) error {
			return ua.StatusBadServiceUnsupported
		},
		"short answer": func(_ context.Context, _ ua.Request, h func(ua.Response) error) error {
			return h(&ua.TransferSubscriptionsResponse{ResponseHeader: &ua.ResponseHeader{}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := NewClient("opc.tcp://example.com:4840")
			require.NoError(t, err)
			out, err := c.transferAndAdopt(context.Background(), []*Subscription{oldSubscription(7, make(chan *PublishNotificationData), 1)}, send)
			require.Error(t, err)
			require.Nil(t, out)
			require.Empty(t, c.SubscriptionIDs())
		})
	}
}

// A subscription id the client already has is refused before anything is
// sent or registered.
func TestTransferSubscriptions_RefusesAnIDAlreadyHeld(t *testing.T) {
	c, err := NewClient("opc.tcp://example.com:4840")
	require.NoError(t, err)
	stub := &stubClient{}
	held := newTestSubscription(t, c, stub, 7)
	_, err = c.transferAndAdopt(context.Background(), []*Subscription{oldSubscription(9, nil, 0), oldSubscription(7, nil, 0)},
		func(context.Context, ua.Request, func(ua.Response) error) error {
			t.Fatal("nothing is sent")
			return nil
		})
	require.Error(t, err)
	c.subMux.RLock()
	defer c.subMux.RUnlock()
	require.Same(t, held, c.subs[7])
	require.NotContains(t, c.subs, uint32(9))
}

// Every notification carries the sequence number of the message it came
// in.
func TestNotifySubscription_CarriesTheSequenceNumber(t *testing.T) {
	c, err := NewClient("opc.tcp://example.com:4840")
	require.NoError(t, err)
	ch := make(chan *PublishNotificationData, 4)
	sub := &Subscription{SubscriptionID: 3, Notifs: ch}
	c.notifySubscription(context.Background(), sub, &ua.NotificationMessage{
		SequenceNumber: 17,
		NotificationData: []*ua.ExtensionObject{
			{Value: &ua.DataChangeNotification{}},
			{Value: &ua.DataChangeNotification{}},
		},
	})
	require.Len(t, ch, 2)
	for range 2 {
		n := <-ch
		require.Equal(t, uint32(17), n.SequenceNumber)
		require.Equal(t, uint32(3), n.SubscriptionID)
	}
}

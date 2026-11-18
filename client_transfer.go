package opcua

import (
	"context"

	"github.com/maestrohub-labs/opcua/errors"
	"github.com/maestrohub-labs/opcua/stats"
	"github.com/maestrohub-labs/opcua/ua"
)

// TransferredSubscription is the server's answer for one subscription of a
// TransferSubscriptions call.
type TransferredSubscription struct {
	// Subscription is the subscription, now this client's: its
	// notifications reach the Notifs channel of the one it was transferred
	// from. nil when the server did not transfer it (Status).
	Subscription *Subscription
	// Status is the server's result for it: Good, or why it was not
	// transferred (Bad_SubscriptionIdInvalid: the server no longer has it;
	// Bad_UserAccessDenied: another user's).
	Status ua.StatusCode
	// AvailableSequenceNumbers are the NotificationMessages the server
	// still holds for it, sent and not acknowledged (Part 4, 5.13.7): the
	// ones Republish can return. Only for a transferred subscription.
	AvailableSequenceNumbers []uint32
}

// TransferSubscriptions moves subscriptions that another client created,
// on an earlier session of the same user, to this client's session (Part 4,
// 5.13.7): an owner that lost its link and reconnected with a new client
// gets the subscriptions the server kept, with what they queued meanwhile.
// SendInitialValues is false: the transferred subscription goes on where it
// stopped.
//
// Each one becomes a subscription of this client with the same id,
// parameters and Notifs channel, and is registered BEFORE the request goes
// out: from the moment the server has transferred it, it may answer this
// session's Publish requests with its notifications, and an unknown one
// would be dropped by the publish loop. The ones the server did not
// transfer are forgotten again. The publish loop is resumed when at least
// one was transferred.
//
// The subscriptions passed in are not used again by their own client: it
// is expected to be closed. Republish then asks for what the server still
// holds (AvailableSequenceNumbers); each PublishNotificationData carries
// its SequenceNumber, so a caller tells a message it already has.
//
// A subscription id this client already has is refused without asking the
// server (an error for the whole call): two subscriptions cannot share an
// id on one client.
func (c *Client) TransferSubscriptions(ctx context.Context, subs []*Subscription) ([]TransferredSubscription, error) {
	return c.transferAndAdopt(ctx, subs, c.Send)
}

// transferAndAdopt is TransferSubscriptions with the request's transport
// given: tests answer it without a server.
func (c *Client) transferAndAdopt(ctx context.Context, subs []*Subscription, send func(context.Context, ua.Request, func(ua.Response) error) error) ([]TransferredSubscription, error) {
	stats.Client().Add("TransferSubscriptions", 1)
	if len(subs) == 0 {
		return nil, nil
	}

	ids := make([]uint32, len(subs))
	adopted := make([]*Subscription, len(subs))
	c.subMux.Lock()
	for i, old := range subs {
		if old == nil {
			c.subMux.Unlock()
			return nil, errors.Errorf("subscription %d is nil", i)
		}
		if _, ok := c.subs[old.SubscriptionID]; ok {
			c.subMux.Unlock()
			return nil, errors.Errorf("subscription %d is already this client's", old.SubscriptionID)
		}
	}
	for i, old := range subs {
		ids[i] = old.SubscriptionID
		adopted[i] = old.adoptedBy(c)
		c.subs[old.SubscriptionID] = adopted[i]
	}
	c.updatePublishTimeout_NeedsSubMuxLock()
	// A publish already in flight may come back Bad_NoSubscription from
	// before the transfer: the loop must not pause on it (monitorSubscriptions).
	c.subsGen.Add(1)
	c.subMux.Unlock()

	forget := func(which func(i int) bool) {
		c.subMux.Lock()
		defer c.subMux.Unlock()
		for i, id := range ids {
			if which(i) && c.subs[id] == adopted[i] {
				delete(c.subs, id)
			}
		}
		c.updatePublishTimeout_NeedsSubMuxLock()
	}

	req := &ua.TransferSubscriptionsRequest{SubscriptionIDs: ids, SendInitialValues: false}
	var res *ua.TransferSubscriptionsResponse
	err := send(ctx, req, func(v ua.Response) error { return safeAssign(v, &res) })
	if err == nil && res != nil && res.ResponseHeader != nil && res.ResponseHeader.ServiceResult != ua.StatusOK {
		err = res.ResponseHeader.ServiceResult
	}
	if err == nil && (res == nil || len(res.Results) != len(ids)) {
		n := 0
		if res != nil {
			n = len(res.Results)
		}
		err = errors.Errorf("TransferSubscriptions answered %d results for %d subscriptions", n, len(ids))
	}
	if err != nil {
		forget(func(int) bool { return true })
		return nil, err
	}

	out := make([]TransferredSubscription, len(ids))
	moved := false
	for i, r := range res.Results {
		out[i].Status = ua.StatusBadUnexpectedError
		if r != nil {
			out[i].Status = r.StatusCode
			out[i].AvailableSequenceNumbers = r.AvailableSequenceNumbers
		}
		if out[i].Status == ua.StatusOK {
			out[i].Subscription = adopted[i]
			moved = true
		}
	}
	forget(func(i int) bool { return out[i].Subscription == nil })
	if moved {
		stats.Subscription().Add("Count", int64(countTransferred(out)))
		c.resumeSubscriptions(ctx)
	}
	return out, nil
}

func countTransferred(out []TransferredSubscription) int {
	n := 0
	for _, t := range out {
		if t.Subscription != nil {
			n++
		}
	}
	return n
}

// adoptedBy is s as a subscription of c: the same id, server-revised
// values, parameters, monitored items, Notifs and sequence position.
func (s *Subscription) adoptedBy(c *Client) *Subscription {
	s.paramsMu.Lock()
	params := s.params
	s.paramsMu.Unlock()
	s.itemsMu.Lock()
	items := make(map[uint32]*monitoredItem, len(s.items))
	for k, v := range s.items {
		items[k] = v
	}
	s.itemsMu.Unlock()
	lastSeq, nextSeq := s.seqs()
	return &Subscription{
		SubscriptionID:            s.SubscriptionID,
		RevisedPublishingInterval: s.RevisedPublishingInterval,
		RevisedLifetimeCount:      s.RevisedLifetimeCount,
		RevisedMaxKeepAliveCount:  s.RevisedMaxKeepAliveCount,
		Notifs:                    s.Notifs,
		params:                    params,
		items:                     items,
		lastSeq:                   lastSeq,
		nextSeq:                   nextSeq,
		c:                         c,
	}
}

// seqs reads the sequence position under its client's lock, when the
// client is one (the publish loop updates it there).
func (s *Subscription) seqs() (last, next uint32) {
	if owner, ok := s.c.(*Client); ok && owner != nil {
		owner.subMux.RLock()
		defer owner.subMux.RUnlock()
	}
	return s.lastSeq, s.nextSeq
}

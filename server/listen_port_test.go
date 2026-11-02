package server_test

import (
	"context"
	"net"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/maestrohub-labs/opcua"
	"github.com/maestrohub-labs/opcua/server"
	"github.com/maestrohub-labs/opcua/ua"
)

// A server configured on port 0 binds a port the OS picks and announces
// that port: URLs, Addr, GetEndpoints and a client's session all name it,
// so a caller needs no probe-then-bind "free port" (another process can
// take a probed port before the server binds it).
func TestServer_Port0ListensOnTheOSPortAndAnnouncesIt(t *testing.T) {
	srv := server.New(
		server.EndPoint("127.0.0.1", 0),
		server.EndPoint("127.0.0.1", 4999), // advertise-only: kept as given
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous),
	)
	if addr := srv.Addr(); addr != nil {
		t.Fatalf("Addr before Start = %v, want nil", addr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	port := srv.Addr().(*net.TCPAddr).Port
	if port == 0 {
		t.Fatal("Addr reports port 0 after Start")
	}
	want := "opc.tcp://127.0.0.1:" + strconv.Itoa(port)
	urls := srv.URLs()
	if len(urls) != 2 || urls[0] != want || urls[1] != "opc.tcp://127.0.0.1:4999" {
		t.Fatalf("URLs = %v, want [%s opc.tcp://127.0.0.1:4999]", urls, want)
	}

	eps, err := opcua.GetEndpoints(ctx, urls[0])
	if err != nil {
		t.Fatalf("GetEndpoints on %s: %v", urls[0], err)
	}
	var sawBound bool
	for _, ep := range eps {
		u, err := url.Parse(ep.EndpointURL)
		if err != nil {
			t.Fatal(err)
		}
		if u.Port() == "0" {
			t.Fatalf("GetEndpoints announces port 0: %s", ep.EndpointURL)
		}
		sawBound = sawBound || ep.EndpointURL == want
	}
	if !sawBound {
		t.Fatalf("GetEndpoints does not announce %s: %v", want, eps)
	}

	c, err := opcua.NewClient(urls[0], opcua.SecurityMode(ua.MessageSecurityModeNone), opcua.AuthAnonymous())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("connect to %s: %v", urls[0], err)
	}
	_ = c.Close(ctx)
}

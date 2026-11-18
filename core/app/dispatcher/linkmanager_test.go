package dispatcher

import (
	"context"
	stdnet "net"
	"testing"
	"testing/synctest"

	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/format"
	"github.com/wyx2685/v2node/limiter"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport/pipe"
)

func TestDeleteUserClosesTransportAfterHalfClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const tag = "test"
		user := panel.UserInfo{Uuid: "user", Id: 1}
		email := format.UserTag(tag, user.Uuid)
		limiter.Init()
		limiter.AddLimiter("vmess", tag, []panel.UserInfo{user}, nil)
		defer limiter.DeleteLimiter(tag)

		conn, peer := stdnet.Pipe()
		defer peer.Close()
		defer conn.Close()
		lifetime, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx := session.ContextWithInbound(lifetime, &session.Inbound{
			Conn: conn, Tag: tag, User: &protocol.MemoryUser{Email: email},
			Source: net.TCPDestination(net.ParseAddress("127.0.0.1"), 12345),
		})
		d := new(DefaultDispatcher)
		d.ActivateUser(email, user.Id)
		inbound, outbound, _, err := d.getLink(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := common.Close(inbound.Writer); err != nil {
			t.Fatal(err)
		}
		if err := outbound.Writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("downlink"))}); err != nil {
			t.Fatal(err)
		}
		if err := common.Close(outbound.Writer); err != nil {
			t.Fatal(err)
		}
		closeLinks := d.DetachUser(email, true)
		if closeLinks == nil {
			t.Fatal("transport was lost after the uplink closed")
		}
		closeLinks()
		if _, err := peer.Write([]byte{1}); err == nil {
			t.Fatal("tracked transport remained open")
		}
		if _, _, _, err := d.getLink(ctx); err == nil {
			t.Fatal("deleted user could still dispatch")
		}
	})
}

func TestFailedFirstWriteReleasesLinkAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager := new(userLinks)
		reader, writer := pipe.New()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pipes := &trackedPipes{manager: manager, uplink: writer, ctx: ctx}
		if !manager.add(&pipes.link) {
			t.Fatal("pipes were rejected")
		}
		reader.Interrupt()
		if err := pipes.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("request"))}); err == nil {
			t.Fatal("write to interrupted pipe succeeded")
		}
		cancel()
		synctest.Wait()
		if closeLinks := manager.retire(); closeLinks != nil {
			t.Fatal("expired transport was retained")
		}
	})
}

func TestKickRetiresOnlyPreviousLinks(t *testing.T) {
	d := new(DefaultDispatcher)
	d.ActivateUser("user", 10)
	value, _ := d.users.Load("user")
	old := value.(*userLinks)
	conn, peer := stdnet.Pipe()
	defer peer.Close()
	defer conn.Close()
	if !old.add(&trackedLink{conn: conn}) {
		t.Fatal("old generation rejected an active connection")
	}
	closeLinks := d.DetachUser("user", false)
	if old.add(new(trackedLink)) {
		t.Fatal("old generation accepted a late registration")
	}
	value, _ = d.users.Load("user")
	current := value.(*userLinks)
	link := new(trackedLink)
	if d.UserID("user") != 10 || !current.add(link) {
		t.Fatal("kick rejected a new connection")
	}
	closeLinks()
	if _, err := peer.Write([]byte{1}); err == nil {
		t.Fatal("kick left old connection open")
	}
	current.remove(link)
	if pending := d.DetachUser("user", true); pending != nil {
		t.Fatal("completed synchronous link was retained")
	}
}

func TestTrackedPipesWaitForDownlinkEOF(t *testing.T) {
	manager := new(userLinks)
	_, up := pipe.New()
	reader, writer := pipe.New()
	pipes := &trackedPipes{manager: manager, uplink: up, link: trackedLink{downlink: reader}}
	if !manager.add(&pipes.link) {
		t.Fatal("pipes were rejected")
	}
	_ = pipes.Close()
	pipes.Interrupt()
	if len(manager.links) != 1 {
		t.Fatal("closing the same direction twice removed the whole link")
	}
	_ = writer.Close()
	if _, err := (*trackedDownlink)(pipes).ReadMultiBuffer(); err == nil {
		t.Fatal("expected EOF after closing the downlink")
	}
	if len(manager.links) != 0 {
		t.Fatal("EOF failed to release the completed link")
	}
}

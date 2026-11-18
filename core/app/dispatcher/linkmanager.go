package dispatcher

import (
	"context"
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/pipe"
)

type userLinks struct {
	uid int

	mu      sync.Mutex
	retired bool
	links   map[*trackedLink]struct{}
}

type trackedLink struct {
	conn     net.Conn
	reader   buf.Reader
	writer   buf.Writer
	downlink *pipe.Reader
	cancel   context.CancelFunc
}

func (m *userLinks) add(link *trackedLink) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.retired {
		return false
	}
	if m.links == nil {
		m.links = make(map[*trackedLink]struct{})
	}
	m.links[link] = struct{}{}
	return true
}

func (m *userLinks) remove(link *trackedLink) {
	m.mu.Lock()
	delete(m.links, link)
	m.mu.Unlock()
}

// Both directions must finish before the flow can be removed. In particular, the
// downlink remains active while the inbound is draining a closed output pipe.
type trackedPipes struct {
	link    trackedLink
	manager *userLinks
	uplink  *pipe.Writer
	ctx     context.Context
	// The manager mutex protects the termination state.
	ended         uint8
	cancelCleanup func() bool
}

const (
	uplinkEnded uint8 = 1 << iota
	downlinkEnded
)

func (p *trackedPipes) finish(direction uint8, writeFailed bool) {
	m := p.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if p.ended&direction != 0 || m.retired {
		return
	}
	p.ended |= direction
	if p.ended == uplinkEnded|downlinkEnded {
		delete(m.links, &p.link)
		if p.cancelCleanup != nil {
			p.cancelCleanup()
		}
	} else if writeFailed && p.ctx != nil {
		// Some inbounds return immediately after a failed first write.
		p.cancelCleanup = context.AfterFunc(p.ctx, func() { m.remove(&p.link) })
	}
}

func (p *trackedPipes) WriteMultiBuffer(mb buf.MultiBuffer) error {
	err := p.uplink.WriteMultiBuffer(mb)
	if err != nil {
		p.finish(uplinkEnded, true)
	}
	return err
}

func (p *trackedPipes) Close() error {
	err := p.uplink.Close()
	p.finish(uplinkEnded, false)
	return err
}

func (p *trackedPipes) Interrupt() {
	p.uplink.Interrupt()
	p.finish(uplinkEnded, false)
}

type trackedDownlink trackedPipes

func (r *trackedDownlink) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.link.downlink.ReadMultiBuffer()
	if err != nil {
		(*trackedPipes)(r).finish(downlinkEnded, false)
	}
	return mb, err
}

func (r *trackedDownlink) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	mb, err := r.link.downlink.ReadMultiBufferTimeout(timeout)
	if err != nil && err != buf.ErrReadTimeout {
		(*trackedPipes)(r).finish(downlinkEnded, false)
	}
	return mb, err
}

func (r *trackedDownlink) Interrupt() {
	common.Interrupt(r.link.downlink)
	(*trackedPipes)(r).finish(downlinkEnded, false)
}

// XUDP reuses a raw pipe reader across inbound connections and sends it
// temporary EOF signals. Track that path by its outbound writer instead.
type trackedXUDPWriter trackedPipes

func (w *trackedXUDPWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	return w.link.writer.WriteMultiBuffer(mb)
}

func (w *trackedXUDPWriter) Close() error {
	err := common.Close(w.link.writer)
	(*trackedPipes)(w).finish(downlinkEnded, false)
	return err
}

func (w *trackedXUDPWriter) Interrupt() {
	common.Interrupt(w.link.writer)
	(*trackedPipes)(w).finish(downlinkEnded, false)
}

func (m *userLinks) retire() func() {
	m.mu.Lock()
	m.retired = true
	links := m.links
	m.links = nil
	m.mu.Unlock()

	if len(links) == 0 {
		return nil
	}
	return func() {
		closed := make(map[net.Conn]struct{})
		for link := range links {
			if link.cancel != nil {
				link.cancel()
			}
		}
		for link := range links {
			if _, ok := closed[link.conn]; link.conn != nil && !ok {
				closed[link.conn] = struct{}{}
				_ = link.conn.Close()
			}
		}
		for link := range links {
			common.Interrupt(link.reader)
			if link.downlink != nil {
				link.downlink.Interrupt()
			}
			common.Interrupt(link.writer)
		}
	}
}

// ActivateUser publishes the connection registry for a newly authorized user.
func (d *DefaultDispatcher) ActivateUser(email string, uid int) {
	d.users.Store(email, &userLinks{uid: uid})
}

func (d *DefaultDispatcher) UserID(email string) int {
	if value, ok := d.users.Load(email); ok {
		return value.(*userLinks).uid
	}
	return 0
}

// DetachUser retires the current connection generation immediately and returns
// the socket and pipe cleanup to run outside the caller's user update lock.
func (d *DefaultDispatcher) DetachUser(email string, revoke bool) func() {
	value, ok := d.users.Load(email)
	if !ok {
		return nil
	}
	manager := value.(*userLinks)
	closeLinks := manager.retire()
	if revoke {
		d.users.Delete(email)
	} else {
		d.ActivateUser(email, manager.uid)
	}
	return closeLinks
}

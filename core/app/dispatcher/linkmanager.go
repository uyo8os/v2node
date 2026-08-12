package dispatcher

import (
	sync "sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
)

type ManagedWriter struct {
	writer  buf.Writer
	manager *LinkManager
	// onClose 在连接正常结束（协议层调用 Close/Interrupt）时触发，
	// 用于从 LinkManager.conns 中移除该连接的原始 socket 引用，
	// 防止长期在线的正常用户因为不断新建/结束连接而导致 conns 表
	// 无限增长（内存泄漏）。为 nil 时表示无需额外清理。
	onClose func()
}

func (w *ManagedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	return w.writer.WriteMultiBuffer(mb)
}

func (w *ManagedWriter) Close() error {
	w.manager.RemoveWriter(w)
	if w.onClose != nil {
		w.onClose()
	}
	return common.Close(w.writer)
}

type LinkManager struct {
	links  map[*ManagedWriter]buf.Reader
	// conns 记录该用户名下所有已建立连接的原始 TCP/Unix socket（session.Inbound.Conn），
	// value 为引用计数而非简单的存在标记。原因：VLESS/Mux 等协议支持在同一条物理连接上
	// 承载多个并发子请求（common/mux.ServerWorker.handleStatusNew 会为每个子流拷贝一份
	// session.Inbound，但其中的 Conn 字段值仍指向同一个底层连接），此时多个子流会共享
	// 同一个 net.Conn 并各自调用 AddConn/RemoveConn。若用简单 set 语义，子流 A 提前结束
	// 会把整条物理连接的记录直接删除，导致子流 B/C 仍在使用时这条连接却已经"脱离监管"，
	// 真正需要强制下线时反而漏关。改为引用计数后，只有当所有共享该连接的子流都结束时，
	// 计数归零才真正移除记录，避免这种误删。
	//
	// 仅关闭上面注册的 pipe Reader/Writer 无法保证真正断开连接：xray-core 各协议
	// 内部实现（例如 VLESS XTLS Vision 的 unsafe 指针直读原始 conn、BufferedWriter
	// 缓冲写入等）会绕过我们包装的抽象层，直接操作底层 socket。只有在操作系统层面
	// 强制关闭原始连接，才能保证不管协议层内部如何拷贝数据，连接都必定被物理断开。
	conns  map[net.Conn]int
	mu     sync.RWMutex
	closed bool
}

func (m *LinkManager) AddLink(writer *ManagedWriter, reader buf.Reader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.links[writer] = reader
	}
}

func (m *LinkManager) RemoveWriter(writer *ManagedWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		delete(m.links, writer)
	}
}

// AddConn 注册该用户连接对应的原始 socket（引用计数 +1）。若 LinkManager 已被关闭
// （用户已被踢下线后又有残留请求进来），立即关闭该连接，不允许新连接借助已删除用户
// 的名义继续存活。
func (m *LinkManager) AddConn(conn net.Conn) {
	if conn == nil {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		conn.Close()
		return
	}
	if m.conns == nil {
		m.conns = make(map[net.Conn]int)
	}
	m.conns[conn]++
	m.mu.Unlock()
}

// RemoveConn 在某个使用该连接的子流正常结束时递减引用计数，仅当计数归零
// （即所有共享这个物理连接的子流都已结束）时才真正从 conns 表移除该连接，
// 避免正常用户的历史连接记录无限堆积，同时不会误删仍被 Mux 其他子流占用的连接。
func (m *LinkManager) RemoveConn(conn net.Conn) {
	if conn == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.conns == nil {
		return
	}
	if n, ok := m.conns[conn]; ok {
		if n <= 1 {
			delete(m.conns, conn)
		} else {
			m.conns[conn] = n - 1
		}
	}
}

func (m *LinkManager) CloseAll() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true

	links := m.links
	m.links = make(map[*ManagedWriter]buf.Reader)
	conns := m.conns
	m.conns = nil
	m.mu.Unlock()

	for w, r := range links {
		common.Close(w.writer)
		common.Interrupt(r)
	}
	// 直接关闭底层原始连接：这是操作系统级别的强制中断，无论协议层内部
	// 采用何种方式拷贝/转发数据，都无法绕开这一步，从而保证已建立的连接
	// （下载、视频流等）在用户被删除时必定被真正断开。
	for c := range conns {
		c.Close()
	}
}

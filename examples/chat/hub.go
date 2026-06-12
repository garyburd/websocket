package main

import "sync"

// Hub maintains the set of active peers and broadcasts messages to them.
// Methods may be called concurrently from any goroutine; the mutex guards
// the peers map.
type Hub struct {
	mu    sync.Mutex
	peers map[*Peer]bool
}

func newHub() *Hub {
	return &Hub{
		peers: make(map[*Peer]bool),
	}
}

// register adds a peer to the hub.
func (h *Hub) register(p *Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.peers[p] = true
}

// unregister removes a peer from the hub and closes its send channel to
// signal that no more messages will be sent to it. The membership check makes
// it a no-op if the peer was already dropped by broadcast; closing only here,
// under the mutex and after that check, closes the channel exactly once.
func (h *Hub) unregister(p *Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.peers[p]; ok {
		delete(h.peers, p)
		close(p.send)
	}
}

// broadcast sends message to every registered peer with a non-blocking send.
// A peer whose send buffer is full is assumed dead or stuck and is dropped
// (its send channel closed, as in unregister). The non-blocking send bounds the
// time the mutex is held, so a slow peer can never block the hub.
func (h *Hub) broadcast(message []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for peer := range h.peers {
		select {
		case peer.send <- message:
		default:
			close(peer.send)
			delete(h.peers, peer)
		}
	}
}

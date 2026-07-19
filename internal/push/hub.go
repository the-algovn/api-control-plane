// Package push fans Kafka sse.* frames out to SSE subscribers via a generic Hub.
package push

import (
	"sort"
	"sync"
)

const subscriberBuffer = 16

type Subscription struct {
	C       <-chan []byte
	ch      chan []byte
	channel string
	hub     *Hub
	once    sync.Once
}

func (s *Subscription) Close() { s.hub.unsubscribe(s) }

type Hub struct {
	mu      sync.Mutex
	subs    map[string]map[*Subscription]struct{}
	onFirst func(string)
	onLast  func(string)
}

func NewHub() *Hub {
	return &Hub{subs: map[string]map[*Subscription]struct{}{}}
}

func (h *Hub) SetBindingHooks(onFirst, onLast func(channel string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onFirst, h.onLast = onFirst, onLast
}

func (h *Hub) Subscribe(channel string) *Subscription {
	s := &Subscription{ch: make(chan []byte, subscriberBuffer), channel: channel, hub: h}
	s.C = s.ch

	h.mu.Lock()
	set, ok := h.subs[channel]
	if !ok {
		set = map[*Subscription]struct{}{}
		h.subs[channel] = set
	}
	set[s] = struct{}{}
	first := len(set) == 1
	onFirst := h.onFirst
	h.mu.Unlock()

	if first && onFirst != nil {
		onFirst(channel)
	}
	return s
}

func (h *Hub) unsubscribe(s *Subscription) {
	h.mu.Lock()
	set, ok := h.subs[s.channel]
	var last bool
	var onLast func(string)
	if ok {
		if _, present := set[s]; present {
			delete(set, s)
			if len(set) == 0 {
				delete(h.subs, s.channel)
				last, onLast = true, h.onLast
			}
		}
	}
	h.mu.Unlock()

	s.once.Do(func() { close(s.ch) })
	if last && onLast != nil {
		onLast(s.channel)
	}
}

// Publish delivers to every subscriber without blocking; a subscriber whose
// buffer is full is evicted (spec: slow clients are disconnected).
func (h *Hub) Publish(channel string, data []byte) {
	h.mu.Lock()
	var evict []*Subscription
	for s := range h.subs[channel] {
		select {
		case s.ch <- data:
		default:
			evict = append(evict, s)
		}
	}
	h.mu.Unlock()
	for _, s := range evict {
		h.unsubscribe(s)
	}
}

func (h *Hub) CloseAll() {
	h.mu.Lock()
	var all []*Subscription
	for _, set := range h.subs {
		for s := range set {
			all = append(all, s)
		}
	}
	h.mu.Unlock()
	for _, s := range all {
		s.hub.unsubscribe(s)
	}
}

func (h *Hub) ActiveChannels() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.subs))
	for ch := range h.subs {
		out = append(out, ch)
	}
	sort.Strings(out)
	return out
}

func (h *Hub) Counts() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int, len(h.subs))
	for ch, set := range h.subs {
		out[ch] = len(set)
	}
	return out
}

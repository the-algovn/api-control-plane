package push

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func recvOne(t *testing.T, c <-chan []byte) []byte {
	t.Helper()
	select {
	case b, ok := <-c:
		require.True(t, ok, "subscription closed unexpectedly")
		return b
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return nil
	}
}

func TestHub_PublishFanOut(t *testing.T) {
	h := NewHub()
	s1 := h.Subscribe("demo.ping")
	s2 := h.Subscribe("demo.ping")
	other := h.Subscribe("other.chan")
	defer s1.Close()
	defer s2.Close()
	defer other.Close()

	h.Publish("demo.ping", []byte(`{"n":1}`))
	require.JSONEq(t, `{"n":1}`, string(recvOne(t, s1.C)))
	require.JSONEq(t, `{"n":1}`, string(recvOne(t, s2.C)))
	select {
	case <-other.C:
		t.Fatal("wrong channel received event")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHub_BindingHooks(t *testing.T) {
	h := NewHub()
	var mu sync.Mutex
	var events []string
	h.SetBindingHooks(
		func(ch string) { mu.Lock(); events = append(events, "first:"+ch); mu.Unlock() },
		func(ch string) { mu.Lock(); events = append(events, "last:"+ch); mu.Unlock() },
	)
	s1 := h.Subscribe("a.b")
	s2 := h.Subscribe("a.b")
	s1.Close()
	s2.Close()
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"first:a.b", "last:a.b"}, events)
}

func TestHub_SlowClientEvicted(t *testing.T) {
	h := NewHub()
	s := h.Subscribe("a.b")
	for range 20 { // buffer is 16; overflow forces eviction
		h.Publish("a.b", []byte(`{}`))
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-s.C:
			if !ok {
				return // closed: evicted as expected
			}
		case <-deadline:
			t.Fatal("slow client was not evicted")
		}
	}
}

func TestHub_CloseAllAndActiveChannels(t *testing.T) {
	h := NewHub()
	s := h.Subscribe("a.b")
	require.Equal(t, []string{"a.b"}, h.ActiveChannels())
	require.Equal(t, map[string]int{"a.b": 1}, h.Counts())
	h.CloseAll()
	_, ok := <-s.C
	require.False(t, ok)
	require.Empty(t, h.ActiveChannels())

	// double Close is safe
	s.Close()
}

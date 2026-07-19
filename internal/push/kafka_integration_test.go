//go:build integration

package push

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func startRedpanda(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	c, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	seed, err := c.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return []string{seed}
}

func TestKafkaConsumer_RoutesToHub(t *testing.T) {
	ctx := context.Background()
	brokers := startRedpanda(t)

	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer prod.Close()
	adm := kadm.NewClient(prod)
	for _, tp := range []string{"sse.counter", "sse.user"} {
		_, err := adm.CreateTopic(ctx, 1, 1, nil, tp)
		require.NoError(t, err)
	}

	hub := NewHub()
	routes := []TopicRoute{
		{Topic: "sse.counter", Channel: "the-button.counter"},
		{Topic: "sse.user", Channel: "the-button.user", PerUser: true},
	}
	cons, err := NewKafkaConsumer(brokers, "sse-test-1", hub, routes, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	require.NoError(t, err)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = cons.Run(wctx) }()

	// Subscribe BEFORE producing (consumer reads from latest).
	require.Eventually(t, cons.Connected, 15*time.Second, 100*time.Millisecond)
	counterSub := hub.Subscribe("the-button.counter")
	defer counterSub.Close()
	userSub := hub.Subscribe("the-button.user.u1")
	defer userSub.Close()
	time.Sleep(500 * time.Millisecond) // let the group settle at latest before producing

	require.NoError(t, prod.ProduceSync(ctx,
		&kgo.Record{Topic: "sse.counter", Key: []byte("counter"), Value: []byte(`{"type":"counter","total":7}`)}).FirstErr())
	require.NoError(t, prod.ProduceSync(ctx,
		&kgo.Record{Topic: "sse.user", Key: []byte("u1"), Value: []byte(`{"type":"user","sub":"u1","total":7}`)}).FirstErr())

	select {
	case f := <-counterSub.C:
		require.Contains(t, string(f), `"total":7`)
	case <-time.After(10 * time.Second):
		t.Fatal("no counter frame on the-button.counter")
	}
	select {
	case f := <-userSub.C:
		require.Contains(t, string(f), `"sub":"u1"`)
	case <-time.After(10 * time.Second):
		t.Fatal("no user frame on the-button.user.u1")
	}
}

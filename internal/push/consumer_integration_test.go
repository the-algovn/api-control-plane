//go:build integration

package push

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

func startRabbit(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcrabbit.Run(ctx, "rabbitmq:4.1-management-alpine")
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	url, err := c.AmqpURL(ctx)
	require.NoError(t, err)
	return url
}

func TestConsumer_EndToEnd(t *testing.T) {
	url := startRabbit(t)
	hub := NewHub()
	cons := NewConsumer(url, hub, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cons.Run(ctx)
	require.Eventually(t, cons.Connected, 15*time.Second, 100*time.Millisecond)

	sub := hub.Subscribe("demo.ping") // triggers a bind command
	defer sub.Close()

	pub, err := amqp.Dial(url)
	require.NoError(t, err)
	defer pub.Close()
	pch, err := pub.Channel()
	require.NoError(t, err)

	// binding is asynchronous — publish until the event lands
	var got []byte
	require.Eventually(t, func() bool {
		err := pch.PublishWithContext(context.Background(), "events", "demo.ping", false, false,
			amqp.Publishing{ContentType: "application/json", Body: []byte(`{"total":42}`)})
		require.NoError(t, err)
		select {
		case got = <-sub.C:
			return true
		case <-time.After(300 * time.Millisecond):
			return false
		}
	}, 15*time.Second, 100*time.Millisecond)
	require.JSONEq(t, `{"total":42}`, string(got))

	// events on unbound routing keys don't arrive
	other := hub.Subscribe("other.topic")
	defer other.Close()
	require.NoError(t, pch.PublishWithContext(context.Background(), "events", "unbound.key", false, false,
		amqp.Publishing{Body: []byte(`{}`)}))
	select {
	case <-other.C:
		t.Fatal("received event for a key nobody bound")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestConsumer_ChurnConvergence(t *testing.T) {
	url := startRabbit(t)
	hub := NewHub()
	cons := NewConsumer(url, hub, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cons.Run(ctx)
	require.Eventually(t, cons.Connected, 15*time.Second, 100*time.Millisecond)

	// churn: 150 distinct channels subscribe+close rapidly (floods/overflows cmds)
	for i := range 150 {
		s := hub.Subscribe(fmt.Sprintf("churn.c%d", i))
		s.Close()
	}
	// consumer must still be alive and converge for a fresh channel
	sub := hub.Subscribe("churn.final")
	defer sub.Close()
	pub, err := amqp.Dial(url)
	require.NoError(t, err)
	defer pub.Close()
	pch, err := pub.Channel()
	require.NoError(t, err)
	var got []byte
	require.Eventually(t, func() bool {
		require.NoError(t, pch.PublishWithContext(context.Background(), "events", "churn.final", false, false,
			amqp.Publishing{ContentType: "application/json", Body: []byte(`{"ok":1}`)}))
		select {
		case got = <-sub.C:
			return true
		case <-time.After(300 * time.Millisecond):
			return false
		}
	}, 20*time.Second, 100*time.Millisecond) // > one 5s reconcile tick
	require.JSONEq(t, `{"ok":1}`, string(got))
}

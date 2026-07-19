package push

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TopicRoute maps one Kafka topic to a hub channel. When PerUser is set, the
// record key is the user's sub and the frame is published to
// "<Channel>.<key>" so it reaches only that user's SSE connections.
type TopicRoute struct {
	Topic   string
	Channel string
	PerUser bool
}

// KafkaConsumer consumes the sse.* topics with a per-replica group (from latest)
// and fans each frame into the Hub. It is the Kafka replacement for the retired
// RabbitMQ Consumer; unlike that one it needs no per-channel bind/unbind — every
// replica receives every frame and the Hub drops frames with no local subscriber.
type KafkaConsumer struct {
	cl        *kgo.Client
	hub       *Hub
	routes    map[string]TopicRoute // topic -> route
	logger    *slog.Logger
	connected atomic.Bool
}

func NewKafkaConsumer(brokers []string, groupID string, hub *Hub, routes []TopicRoute, logger *slog.Logger) (*KafkaConsumer, error) {
	topics := make([]string, 0, len(routes))
	m := make(map[string]TopicRoute, len(routes))
	for _, r := range routes {
		topics = append(topics, r.Topic)
		m[r.Topic] = r
	}
	c := &KafkaConsumer{hub: hub, routes: m, logger: logger}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		// Connected() must mean "can actually deliver frames", which requires
		// this replica's group membership to have been assigned partitions —
		// mere broker reachability (e.g. a Ping) is not enough: a consumer
		// group's "latest" offset is only pinned once assignment completes, so
		// gating readiness on anything earlier races with frames published
		// between reachability and assignment (they'd be silently skipped).
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, _ map[string][]int32) {
			c.connected.Store(true)
		}),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	return c, nil
}

func (c *KafkaConsumer) Connected() bool { return c.connected.Load() }

// Run blocks until ctx is done, polling frames and publishing them to the hub.
func (c *KafkaConsumer) Run(ctx context.Context) error {
	defer c.cl.Close()
	for ctx.Err() == nil {
		f := c.cl.PollFetches(ctx)
		if f.IsClientClosed() {
			return nil
		}
		if errs := f.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if !errors.Is(e.Err, context.Canceled) {
					c.logger.Warn("sse kafka fetch", "topic", e.Topic, "err", e.Err)
				}
			}
			continue
		}
		f.EachRecord(func(rec *kgo.Record) {
			route, ok := c.routes[rec.Topic]
			if !ok {
				return
			}
			channel := route.Channel
			if route.PerUser {
				channel = route.Channel + "." + string(rec.Key)
			}
			c.hub.Publish(channel, rec.Value)
		})
	}
	return ctx.Err()
}

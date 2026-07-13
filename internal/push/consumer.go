package push

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const exchange = "events"

// Consumer owns the AMQP connection. All channel operations happen inside
// Run's goroutine; bind/unbind requests arrive over cmds (amqp channels are
// not safe for concurrent use).
type Consumer struct {
	url       string
	hub       *Hub
	logger    *slog.Logger
	cmds      chan string
	connected atomic.Bool
}

func NewConsumer(url string, hub *Hub, logger *slog.Logger) *Consumer {
	c := &Consumer{url: url, hub: hub, logger: logger, cmds: make(chan string, 64)}
	// Both hooks just enqueue the channel name; they do NOT carry a bind/unbind
	// verb. Under a reconnect race, a channel's onLast and a new subscriber's
	// onFirst can be enqueued in an order that inverts by the time the session
	// loop processes them, so trusting the verb could leave a stale bind or
	// miss one. Instead the session loop reconciles against hub.Counts() truth
	// at the moment each command is dequeued. Signals are best-effort hints
	// for low latency; the session's periodic reconcile ticker is the
	// correctness backstop that converges bindings after dropped signals.
	hook := func(ch string) {
		select {
		case c.cmds <- ch:
		default:
			// full: drop the signal — the session's periodic reconcile
			// converges bindings to hub truth, so no signal is load-bearing
		}
	}
	hub.SetBindingHooks(hook, hook)
	return c
}

func (c *Consumer) Connected() bool { return c.connected.Load() }

func (c *Consumer) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if err := c.session(ctx); err != nil {
			c.logger.Warn("rabbitmq session ended", "err", err, "retry_in", backoff)
		}
		c.connected.Store(false)
		c.hub.CloseAll() // spec: broker down => close SSE clients; EventSource reconnects
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		} else {
			backoff = time.Second // periodic reset keeps retries lively
		}
	}
}

func (c *Consumer) session(ctx context.Context) error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	q, err := ch.QueueDeclare("", false, true, true, false, nil) // server-named, auto-delete, exclusive
	if err != nil {
		return err
	}
	bound := map[string]bool{}
	for _, name := range c.hub.ActiveChannels() { // re-bind after reconnect
		if err := ch.QueueBind(q.Name, name, exchange, false, nil); err != nil {
			return err
		}
		bound[name] = true
	}
	msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		return err
	}
	c.connected.Store(true)
	c.logger.Info("rabbitmq connected", "queue", q.Name)

	reconcile := time.NewTicker(5 * time.Second)
	defer reconcile.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-reconcile.C:
			// full reconcile: converges bindings after hook signals dropped
			// on a saturated cmds buffer
			counts := c.hub.Counts()
			for name := range counts {
				if !bound[name] {
					if err := ch.QueueBind(q.Name, name, exchange, false, nil); err != nil {
						return err
					}
					bound[name] = true
				}
			}
			for name := range bound {
				if counts[name] == 0 {
					if err := ch.QueueUnbind(q.Name, name, exchange, nil); err != nil {
						return err
					}
					delete(bound, name)
				}
			}
		case name := <-c.cmds:
			want := c.hub.Counts()[name] > 0
			if want && !bound[name] {
				if err := ch.QueueBind(q.Name, name, exchange, false, nil); err != nil {
					return err
				}
				bound[name] = true
			} else if !want && bound[name] {
				if err := ch.QueueUnbind(q.Name, name, exchange, nil); err != nil {
					return err
				}
				delete(bound, name)
			}
		case m, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}
			c.hub.Publish(m.RoutingKey, m.Body)
		}
	}
}

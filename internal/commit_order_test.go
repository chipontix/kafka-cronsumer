package internal

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
	segmentio "github.com/segmentio/kafka-go"
)

// queueReader hands out msgs in order, then blocks until ctx is done, and
// records the offset of every committed message.
type queueReader struct {
	mu      sync.Mutex
	msgs    []segmentio.Message
	commits []int64
}

func (r *queueReader) FetchMessage(ctx context.Context) (segmentio.Message, error) {
	r.mu.Lock()
	if len(r.msgs) > 0 {
		m := r.msgs[0]
		r.msgs = r.msgs[1:]
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return segmentio.Message{}, ctx.Err()
}

func (r *queueReader) CommitMessages(_ context.Context, msgs ...segmentio.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.commits = append(r.commits, m.Offset)
	}
	return nil
}

func (r *queueReader) Close() error { return nil }

func (r *queueReader) committed() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.commits)
}

// A process crash while consumeFn runs must not lose the record: its offset
// may only be committed after consumeFn returned.
func Test_Listen_Should_Not_Commit_Record_While_Handler_Is_Running(t *testing.T) {
	reader := &queueReader{}
	for i := int64(0); i < 3; i++ {
		reader.msgs = append(reader.msgs, segmentio.Message{Topic: "exception", Partition: 0, Offset: i, Value: []byte("v")})
	}

	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	var once sync.Once
	cfg := &kafka.Config{Consumer: kafka.ConsumerConfig{Topic: "exception"}, Logger: logger.New("error")}
	c := &cronsumer{
		cfg:            cfg,
		messageChannel: make(chan MessageWrapper),
		kafkaConsumer:  &kafkaConsumer{consumer: reader, cfg: cfg},
		kafkaProducer:  &mockProducer{},
		consumeFn: func(_ kafka.Message) error {
			once.Do(func() {
				close(handlerEntered)
				<-releaseHandler
			})
			return nil
		},
		metric:   &CronsumerMetric{},
		maxRetry: 3,
	}
	c.shutdownCtx, c.shutdownCancel = context.WithCancel(context.Background())
	c.sendCond.L = &c.sendMu
	c.SetupConcurrentWorkers(1)

	ctx, cancel := context.WithCancel(context.Background())
	cancelFn := func() { cancel() }
	listened := make(chan struct{})
	go func() {
		defer close(listened)
		c.Listen(ctx, kafka.FixedBackOffStrategy, &cancelFn)
	}()

	select {
	case <-handlerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	// Give the listener time to fetch the next record and block on the
	// handoff, as it does in production.
	time.Sleep(50 * time.Millisecond)
	if got := reader.committed(); len(got) != 0 {
		t.Fatalf("offsets %v committed while the handler of offset 0 is still running", got)
	}

	close(releaseHandler)
	deadline := time.Now().Add(2 * time.Second)
	for !slices.Contains(reader.committed(), 2) {
		if time.Now().After(deadline) {
			t.Fatalf("offset 2 not committed after all handlers returned, commits %v", reader.committed())
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	<-listened
	c.Stop()
}

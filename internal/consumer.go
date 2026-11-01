package internal

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

type Consumer interface {
	ReadMessage(ctx context.Context) (*segmentio.Message, error)
	Ack(msg kafka.Message)
	Stop()
}

type segmentReader interface {
	FetchMessage(ctx context.Context) (segmentio.Message, error)
	CommitMessages(ctx context.Context, msgs ...segmentio.Message) error
	Close() error
}

type kafkaConsumer struct {
	consumer segmentReader
	cfg      *kafka.Config

	// pending holds the offsets of fetched records per partition, in offset
	// order, until they are acked. A partition is committed only up to the
	// first record still being handled, so a crash redelivers it.
	mu      sync.Mutex
	pending map[topicPartition][]pendingRecord
}

type topicPartition struct {
	topic     string
	partition int
}

type pendingRecord struct {
	offset int64
	acked  bool
}

func newConsumer(kafkaConfig *kafka.Config) *kafkaConsumer {
	readerConfig := segmentio.ReaderConfig{
		Brokers:           kafkaConfig.Brokers,
		GroupID:           kafkaConfig.Consumer.GroupID,
		GroupTopics:       []string{kafkaConfig.Consumer.Topic},
		MinBytes:          kafkaConfig.Consumer.MinBytes,
		MaxBytes:          kafkaConfig.Consumer.MaxBytes,
		MaxWait:           kafkaConfig.Consumer.MaxWait,
		CommitInterval:    kafkaConfig.Consumer.CommitInterval,
		HeartbeatInterval: kafkaConfig.Consumer.HeartbeatInterval,
		SessionTimeout:    kafkaConfig.Consumer.SessionTimeout,
		RebalanceTimeout:  kafkaConfig.Consumer.RebalanceTimeout,
		StartOffset:       kafkaConfig.Consumer.StartOffset.Value(),
		RetentionTime:     kafkaConfig.Consumer.RetentionTime,
		QueueCapacity:     kafkaConfig.Consumer.QueueCapacity,
	}

	readerConfig.Dialer = &segmentio.Dialer{
		ClientID: kafkaConfig.Consumer.ClientID,
	}

	if kafkaConfig.SASL.Enabled {
		readerConfig.Dialer.TLS = NewTLSConfig(kafkaConfig)
		readerConfig.Dialer.SASLMechanism = Mechanism(kafkaConfig.SASL)

		if kafkaConfig.SASL.Rack != "" {
			readerConfig.GroupBalancers = []segmentio.GroupBalancer{segmentio.RackAffinityGroupBalancer{Rack: kafkaConfig.SASL.Rack}}
		}
	}

	return &kafkaConsumer{
		consumer: segmentio.NewReader(readerConfig),
		cfg:      kafkaConfig,
	}
}

// ReadMessage fetches the next record without committing it; the caller
// must Ack it once it is handled.
func (k *kafkaConsumer) ReadMessage(ctx context.Context) (*segmentio.Message, error) {
	msg, err := k.consumer.FetchMessage(ctx)
	if err != nil {
		if isContextCancelled(err) {
			k.cfg.Logger.Debug("kafka-go context is cancelled")
			return nil, nil
		}
		return nil, err
	}

	k.mu.Lock()
	if k.pending == nil {
		k.pending = map[topicPartition][]pendingRecord{}
	}
	tp := topicPartition{msg.Topic, msg.Partition}
	q := k.pending[tp]
	// Records of a partition arrive in offset order, but a rebalance can
	// rewind the reader, so insert in place instead of appending.
	i, _ := slices.BinarySearchFunc(q, msg.Offset, func(r pendingRecord, o int64) int {
		if r.offset <= o {
			return -1
		}
		return 1
	})
	k.pending[tp] = slices.Insert(q, i, pendingRecord{offset: msg.Offset})
	k.mu.Unlock()

	return &msg, nil
}

// Ack marks a record returned by ReadMessage as handled and commits its
// partition up to the last record before the first one still in flight.
func (k *kafkaConsumer) Ack(msg kafka.Message) {
	k.mu.Lock()
	defer k.mu.Unlock()

	tp := topicPartition{msg.Topic, msg.Partition}
	q := k.pending[tp]
	for i := range q {
		if q[i].offset == msg.Offset && !q[i].acked {
			q[i].acked = true
			break
		}
	}

	n := 0
	for n < len(q) && q[n].acked {
		n++
	}
	if n == 0 {
		return
	}
	last := q[n-1].offset
	if n == len(q) {
		delete(k.pending, tp)
	} else {
		k.pending[tp] = q[n:]
	}

	// Commit under the lock so offsets reach the reader in order. With the
	// default CommitInterval this only queues the offset; with 0 it waits for
	// the broker.
	if err := k.consumer.CommitMessages(context.Background(), segmentio.Message{
		Topic: msg.Topic, Partition: msg.Partition, Offset: last,
	}); err != nil {
		k.cfg.Logger.Errorf("Error while committing offset %d of %s/%d: %v", last, msg.Topic, msg.Partition, err)
	}
}

func isContextCancelled(err error) bool {
	return errors.Is(err, context.Canceled)
}

func (k *kafkaConsumer) Stop() {
	if err := k.consumer.Close(); err != nil {
		k.cfg.Logger.Errorf("Error while closing kafka consumer %v", err)
	}
}

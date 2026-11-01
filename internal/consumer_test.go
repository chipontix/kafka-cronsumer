package internal

import (
	"context"
	"slices"
	"testing"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"
	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
	segmentio "github.com/segmentio/kafka-go"
)

type mockSegmentReader struct {
	msg        segmentio.Message
	fetchErr   error
	commitErr  error
	onFetched  func()
	commitCtx  context.Context
	commitCall int
}

func (m *mockSegmentReader) FetchMessage(_ context.Context) (segmentio.Message, error) {
	if m.onFetched != nil {
		m.onFetched()
	}
	return m.msg, m.fetchErr
}

func (m *mockSegmentReader) CommitMessages(ctx context.Context, _ ...segmentio.Message) error {
	m.commitCtx = ctx
	m.commitCall++
	return m.commitErr
}

func (m *mockSegmentReader) Close() error {
	return nil
}

func Test_ReadMessage_Should_Return_Fetched_Message_When_Context_Cancelled_After_Fetch(t *testing.T) {
	// Given a fetched record whose listening context gets cancelled
	// immediately after FetchMessage returns (cron window close).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fetched := segmentio.Message{Topic: "exception", Partition: 0, Offset: 9, Key: []byte("record-A")}
	reader := &mockSegmentReader{
		msg: fetched,
		onFetched: func() {
			cancel()
		},
	}
	k := &kafkaConsumer{
		consumer: reader,
		cfg:      &kafka.Config{Logger: logger.New("info")},
	}

	// When
	msg, err := k.ReadMessage(ctx)
	// Then the fetched record must not be discarded, and is committed only on Ack
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if msg == nil {
		t.Fatal("expected fetched message to be returned, got nil")
	}
	if string(msg.Key) != string(fetched.Key) || msg.Offset != fetched.Offset {
		t.Fatalf("expected %+v, got %+v", fetched, *msg)
	}
	if reader.commitCall != 0 {
		t.Fatalf("expected no commit before Ack, got %d", reader.commitCall)
	}

	k.Ack(NewMessageWrapper(*msg, kafka.FixedBackOffStrategy).Message)
	if reader.commitCall != 1 {
		t.Fatalf("expected exactly one commit call, got %d", reader.commitCall)
	}
	if reader.commitCtx == nil || reader.commitCtx.Err() != nil {
		t.Fatal("expected commit to run with a context detached from the cancelled listening context")
	}
}

func Test_Ack_Should_Commit_Only_Up_To_First_Unacked_Record(t *testing.T) {
	// Given three fetched records of one partition
	reader := &queueReader{}
	for i := int64(0); i < 3; i++ {
		reader.msgs = append(reader.msgs, segmentio.Message{Topic: "exception", Partition: 0, Offset: i})
	}
	k := &kafkaConsumer{consumer: reader, cfg: &kafka.Config{Logger: logger.New("info")}}
	var fetched []kafka.Message
	for range 3 {
		m, err := k.ReadMessage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		fetched = append(fetched, NewMessageWrapper(*m, kafka.FixedBackOffStrategy).Message)
	}

	// When they are handled out of order, Then the commit never passes a record in flight
	k.Ack(fetched[1])
	if got := reader.committed(); len(got) != 0 {
		t.Fatalf("offset 0 is in flight, expected no commit, got %v", got)
	}
	k.Ack(fetched[0])
	k.Ack(fetched[2])
	if got := reader.committed(); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("expected commits [1 2], got %v", got)
	}
	if len(k.pending) != 0 {
		t.Fatalf("expected nothing pending, got %v", k.pending)
	}
}

func Test_ReadMessage_Should_Return_Nil_Nil_When_Fetch_Context_Cancelled(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	reader := &mockSegmentReader{fetchErr: context.Canceled}
	k := &kafkaConsumer{
		consumer: reader,
		cfg:      &kafka.Config{Logger: logger.New("info")},
	}

	// When
	msg, err := k.ReadMessage(ctx)

	// Then
	if msg != nil || err != nil {
		t.Fatalf("expected (nil, nil), got (%v, %v)", msg, err)
	}
	if reader.commitCall != 0 {
		t.Fatalf("expected no commit call, got %d", reader.commitCall)
	}
}

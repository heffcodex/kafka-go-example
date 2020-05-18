package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

type Stats struct {
	sync.Mutex

	OverallDuration int `json:"overall_duration"`
	AvgDuration     int `json:"avg_duration"`
	SuccessCount    int `json:"success_count"`
}

func (s *Stats) Add(ms int) {
	s.Lock()
	defer s.Unlock()

	s.OverallDuration += ms
	s.SuccessCount++
	s.AvgDuration = s.OverallDuration / s.SuccessCount
}

var (
	stats Stats
	_rand = rand.New(rand.NewSource(time.Now().UnixNano()))
)

const (
	kafkaHost = "0.0.0.0:29092"
)

func randError() error {
	if _rand.Intn(100) == 1 {
		return errors.New("random error")
	}

	return nil
}

func performTask() (int, error) {
	waitDelay := rand.Intn(1800) + 200
	time.Sleep(time.Duration(waitDelay) * time.Millisecond)

	return waitDelay, randError()
}

func consumer(ctx context.Context, rMessages *kafka.Reader, wResults *kafka.Writer) error {
	for {
		select {
		case <-ctx.Done():
			log.Print("an error occurred in another thread, stopping")
			return nil
		default:
		}

		msg, err := rMessages.FetchMessage(context.TODO())
		if err != nil {
			return err
		}

		elapsed, err := performTask()
		if err != nil {
			return err
		}

		if err := rMessages.CommitMessages(context.TODO(), msg); err != nil {
			return err
		}

		if err := wResults.WriteMessages(context.TODO(), kafka.Message{
			Value: []byte(fmt.Sprintf("`%s`s result", string(msg.Value))),
		}); err != nil {
			return err
		}

		stats.Add(elapsed)
	}
}

func run(threadsCount int) error {
	g, ctx := errgroup.WithContext(context.Background())

	rMessages := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaHost},
		Topic:   "messages",
		GroupID: "1",
	})

	wResults := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaHost},
		Topic:   "results",
	})

	defer rMessages.Close()
	defer wResults.Close()

	for i := 0; i < threadsCount; i++ {
		g.Go(func() error { return consumer(ctx, rMessages, wResults) })
	}

	return g.Wait()
}

func produceMessages() error {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaHost},
		Topic:   "messages",
	})

	defer w.Close()

	var msgs []kafka.Message

	for i := 1; i <= 10000; i++ {
		msgs = append(msgs, kafka.Message{
			Value: []byte(fmt.Sprintf("message #%d", i)),
		})
	}

	return w.WriteMessages(context.TODO(), msgs...)
}

func main() {
	if err := produceMessages(); err != nil {
		panic(err)
	}

	runErr := run(100)

	wStats := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaHost},
		Topic:   "stats",
	})

	defer wStats.Close()

	marshaled, err := json.Marshal(stats)
	if err != nil {
		panic(err)
	}

	if err := wStats.WriteMessages(context.TODO(), kafka.Message{
		Value: marshaled,
	}); err != nil {
		panic(err)
	}

	log.Printf("stopped by `%v`, stats: %s", runErr, string(marshaled))
}

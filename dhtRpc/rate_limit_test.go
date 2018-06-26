package dhtRpc

import (
	"context"
	"testing"
	"time"
)

func TestDHTRateLimit(t *testing.T) {
	// We need a swarm so the query stream has more than one peer to query at a time.
	swarm := createSwarm(t, 128, false)
	defer swarm.Close()
	for _, s := range swarm.servers {
		s.OnQuery("", func(Node, *Query) ([]byte, error) {
			time.Sleep(time.Millisecond)
			return []byte("world"), nil
		})
	}

	const parellel = 4
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	streamFinished := make(chan bool)
	for i := 0; i < parellel; i++ {
		query := Query{
			Command: "hello",
			Target:  swarm.servers[i].ID(),
		}
		go func() {
			defer func() { streamFinished <- true }()
			if err := DiscardStream(swarm.client.Query(ctx, &query, nil)); err != nil {
				t.Errorf("backgrounded query errored: %v", err)
			}
		}()
	}

	var finished int
	ticker := time.NewTicker(time.Millisecond / 4)
	defer ticker.Stop()
	var counts []int
	for {
		select {
		case <-streamFinished:
			if finished++; finished >= parellel {
				t.Log(counts)
				return
			}

		case <-ticker.C:
			if cur, limit := swarm.client.socket.Pending(), swarm.client.concurrency; cur > limit {
				t.Fatalf("DHT currently has %d requests pending, expected <= %d", cur, limit)
			} else {
				counts = append(counts, cur)
			}
		}
	}
}

func TestQueryRateLimit(t *testing.T) {
	// We need a swarm so the query stream has more than one peer to query at a time.
	swarm := createSwarm(t, 128, true)
	defer swarm.Close()
	for _, s := range swarm.servers {
		s.OnQuery("", func(Node, *Query) ([]byte, error) {
			time.Sleep(time.Millisecond)
			return []byte("world"), nil
		})
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	streamFinished := make(chan bool)

	const concurrency = 2
	go func() {
		defer func() { streamFinished <- true }()

		query := Query{Command: "hello", Target: swarm.servers[0].ID()}
		opts := QueryOpts{Concurrency: concurrency}
		if err := DiscardStream(swarm.client.Query(ctx, &query, &opts)); err != nil {
			t.Errorf("backgrounded query errored: %v", err)
		}
	}()

	ticker := time.NewTicker(time.Millisecond / 4)
	defer ticker.Stop()
	var counts []int
	for {
		select {
		case <-streamFinished:
			t.Log(counts)
			return

		case <-ticker.C:
			if cur := swarm.client.socket.Pending(); cur > concurrency {
				t.Fatalf("DHT currently has %d requests pending, expected <= %d", cur, concurrency)
			} else {
				counts = append(counts, cur)
			}
		}
	}
}

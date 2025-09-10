package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/dranikpg/gtrs"
	"github.com/goombaio/namegenerator"
	"github.com/redis/go-redis/v9"
)

const (
	redisAddr = "localhost:6379"
	streamKey = "main-stream"

	streamTTL    = 5 * time.Second
	maxStreamLen = 100000
)

// Our type that is sent in the stream.
type Event struct {
	Name     string
	Priority int
	Time     string
}

func main() {
	rootCtx := context.Background()

	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Ping to check the connection
	if _, err := rdb.Ping(rootCtx).Result(); err != nil {
		log.Fatalf("could not connect to Redis: %v", err)
	}
	log.Println("connected to Redis")

	// Create the stream
	stream := gtrs.NewStream[Event](
		rdb,
		streamKey,
		&gtrs.Options{
			TTL:    streamTTL,
			MaxLen: maxStreamLen,
			Approx: true,
		},
	)

	// Publish messages into the stream
	for range 10000 {
		id, err := stream.Add(rootCtx, Event{
			Name:     randomName(),
			Priority: randomNumber(1, 10),
			Time:     time.Now().Format(time.RFC3339),
		})
		if err != nil {
			log.Printf("error writing to stream: %v\n", err)
			return
		}

		log.Printf("published msg with ID: %s\n", id)
		<-time.After(200 * time.Millisecond)
	}
}

func randomName() string {
	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)

	name := nameGenerator.Generate()
	return name
}

func randomNumber(min, max int) int {
	return min + rand.Intn(max-min+1)
}

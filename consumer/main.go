package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	"github.com/dranikpg/gtrs"
	"github.com/redis/go-redis/v9"
)

const (
	redisAddr         = "localhost:6379"
	groupName         = "group-name"
	streamKey         = "main-stream"
	lastIDConsumerKey = "0-0"

	consumerName = "consumer-name"
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

	// Create consumer group
	mainConsumerGroup := gtrs.NewGroupConsumer[Event](
		rootCtx,
		rdb,
		groupName,
		consumerName,
		streamKey,
		lastIDConsumerKey,
	)

	// Do some recovery when we exit
	defer func() {
		// Lets see where we stopped reading stream
		seenIds := mainConsumerGroup.Close()
		log.Println("main consumer group stream reader stopped on", seenIds)
	}()

	for {
		var msg gtrs.Message[Event] // our message

		select {
		// Consumers just close the stream on close or cancellation without
		// sending any cancellation errors.
		// So lets not forget checking the context ourselves
		case <-rootCtx.Done():
			return

		//! TODO: manage SIGTERM gracefully

		// Block simultaneously on all consumers and wait for first to respond
		case msg = <-mainConsumerGroup.Chan():
		}

		switch errv := msg.Err.(type) {
		// This interface-nil comparison in safe. Consumers never return typed nil errors.
		case nil:
			// Handle and ACK the message
			err := handleMessage(msg)
			if err != nil {
				log.Printf("error handling message %s: %v\n", msg.ID, err)
				continue
				//! The `checker` process will be reprocessing the NO_ACK messages
			}
			mainConsumerGroup.Ack(msg)
		case gtrs.ReadError:
			// One of the consumers will stop, so lets stop altogether
			log.Printf("read error! %v Exiting...\n", msg.Err)
			return
		case gtrs.AckError:
			// We can identify the failed ack by stream & id
			log.Printf("ack failed %v-%v :( \n", msg.Stream, msg.ID)
		case gtrs.ParseError:
			// We can do something useful with errv.Data
			log.Printf("parse failed: raw data: %v", errv.Data)
		}
	}
}

func handleMessage(msg gtrs.Message[Event]) error {
	log.Printf(
		"handling event %v from %v\n",
		msg.ID,
		msg.Stream,
	)

	// Create a random number between 0 and 10
	randomNumber := rand.Intn(10)
	if randomNumber < 2 {
		//! Randomly reject the message
		return fmt.Errorf("message rejected")
	}

	return nil
}

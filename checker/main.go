package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/dranikpg/gtrs"
	"github.com/dranikpg/gtrs/gtrsconvert"
	"github.com/redis/go-redis/v9"
)

const (
	redisAddr = "localhost:6379"
	groupName = "group-name"
	streamKey = "main-stream"

	// A unique name for this checker process
	checkerConsumerName = "checker-consumer-name"
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

	// Create a ticker that fires every 5 seconds
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Printf("starting pending message checker for group '%s' on stream '%s'...\n", groupName, streamKey)

	// Loop indefinitely, running on each tick
	for range ticker.C {
		log.Println("checking pending messages...")
		checkAndProcessPending(rootCtx, rdb)
	}
}

func checkAndProcessPending(ctx context.Context, rdb *redis.Client) {
	// 1. Check for pending messages using XPendingExt.
	// We want a detailed list of pending messages, so we provide a start and end.
	// '-' and '+' are special IDs meaning the smallest and largest possible IDs.
	pendingResult, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  "-",
		End:    "+",
		Count:  10, // Process up to 10 messages per check
	}).Result()

	if err != nil {
		log.Printf("error checking for pending messages: %v", err)
		return
	}

	if len(pendingResult) == 0 {
		log.Println("no pending messages found...")
		return
	}

	log.Printf("found %d pending messages. Attempting to claim...\n", len(pendingResult))

	// Collect the IDs of the pending messages
	var messageIDs []string
	for _, p := range pendingResult {
		messageIDs = append(messageIDs, p.ID)
	}

	// 2. Claim the messages that have been idle for at least 10 seconds.
	// The MinIdle duration is crucial to prevent this checker from stealing messages
	// that a normal consumer is actively processing.
	claimResult, err := rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   streamKey,
		Group:    groupName,
		Consumer: checkerConsumerName,
		MinIdle:  10 * time.Second, // Only claim messages idle for more than 10s
		Messages: messageIDs,
	}).Result()

	if err != nil {
		// redis.Nil means no messages were claimed (e.g., they weren't idle long enough)
		if err == redis.Nil {
			log.Println("no messages were old enough to be claimed")
			return
		}
		log.Printf("error claiming messages: %v", err)
		return
	}

	if len(claimResult) == 0 {
		log.Println("although pending messages exist, none were idle long enough to be claimed")
		return
	}

	// 3. Process and Acknowledge (if are succesfully handled) the claimed messages
	log.Printf("successfully claimed %d messages\n", len(claimResult))
	for _, msg := range claimResult {
		log.Printf("processing claimed message ID: %s, Data: %v", msg.ID, msg.Values)

		gtrsMessage := toGtrsMessage[Event](msg, streamKey)

		err := handleMessage(gtrsMessage)
		if err != nil {
			log.Printf("message %s not processed ⛔️\n", msg.ID)
			continue
		}

		// 4. Acknowledge the message so it's removed from the pending list
		ackResult, err := rdb.XAck(ctx, streamKey, groupName, msg.ID).Result()
		if err != nil {
			log.Printf("failed to ACK message %s: %v", msg.ID, err)
		} else {
			log.Printf("successfully processed and ACKed message %s (ACK result: %d).", msg.ID, ackResult)
		}
	}
}

func handleMessage(msg gtrs.Message[Event]) error {
	randomNumber := rand.Intn(10)
	if randomNumber < 2 {
		//! Randomly reject the message
		return fmt.Errorf("message rejected due to random number: %d", randomNumber)
	}

	log.Printf("message %s succesfully processed ✅\n", msg.ID)

	return nil
}

// Convert a redis.XMessage to a Message[T]
func toGtrsMessage[T any](rm redis.XMessage, stream string) gtrs.Message[T] {
	var data T
	var err error

	if err = gtrsconvert.MapToStruct(&data, rm.Values); err != nil {
		err = gtrs.ParseError{
			Data: rm.Values,
			Err:  err,
		}
	}

	return gtrs.Message[T]{
		ID:     rm.ID,
		Stream: stream,
		Err:    err,
		Data:   data,
	}
}

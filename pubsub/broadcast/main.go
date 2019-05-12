package main

import (
	"fmt"
	"time"
	"context"
	"github.com/jasonsoft/log"
	"github.com/jasonsoft/log/handlers/console"	
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"
)

var topicURL = "rabbit://topicA"
var subscriptionURL = "rabbit://"

func main() {
	log.SetAppID("pubsub") // unique id for the app

	clog := console.New()
	log.RegisterHandler(clog, log.AllLevels...)

	ctx := context.Background()
	topic, err := pubsub.OpenTopic(ctx, topicURL)
	if err != nil {
		panic(err)
	}
	defer topic.Shutdown(ctx)

	go writeLoop(ctx, topic)
	go readLoop(ctx, "worker1", true)
	go readLoop(ctx, "worker2", true)

	time.Sleep(10 * time.Hour)
}


func readLoop(ctx context.Context, worker string, doAck bool) error {
	// Create a subscription connected to that topic.
	subscription, err := pubsub.OpenSubscription(ctx, subscriptionURL + worker)
	if err != nil {
		return err
	}

	for {
		msg, err := subscription.Receive(ctx)
		if err != nil {
			// Errors from Receive indicate that Receive will no longer succeed.
			log.Errorf("%s: Receiving message: %v", worker, err)
			return err
		}
		// Do work based on the message, for example:
		log.Infof("%s: Got message: %q, metadata: %v\n", worker, msg.Body, msg.Metadata)
		// Messages must always be acknowledged with Ack.
		if doAck{
			msg.Ack()
		}
		
	}
}

func writeLoop(ctx context.Context, topic *pubsub.Topic) {
	// send message
	i := 1
	for {
		msg := fmt.Sprintf("Hello World - %d", i)
		err := topic.Send(ctx, &pubsub.Message{
			Body: []byte(msg),
			Metadata: map[string]string{
				// These are examples of metadata.
				// There is nothing special about the key names.
				"language":   "en",
				"importance": "high",
			},
		})
		if err != nil {
			log.Errorf("main: send topic err: %v", err)
			break
		}
		i++
		time.Sleep(2 * time.Second)
	}

}
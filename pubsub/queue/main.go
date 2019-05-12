package main

import (
	"fmt"
	"time"
	"context"
	"github.com/jasonsoft/log"
	"github.com/jasonsoft/log/handlers/console"	
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"
	_ "gocloud.dev/pubsub/gcppubsub"
)

// var topicURL = "gcppubsub://transfer-stage/jason-test"
// var subscriptionURL = "gcppubsub://transfer-stage/jason-test"

var topicURL = "rabbit://topicA"
var subscriptionURL = "rabbit://sub1"

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

	subscription, err := pubsub.OpenSubscription(ctx, subscriptionURL)
	if err != nil {
		panic(err)
	}

	go readLoop(ctx, "workder1", subscription, true)
	go readLoop(ctx, "workder2", subscription, true)

	time.Sleep(10 * time.Hour)
}


func readLoop(ctx context.Context, worker string, subscription *pubsub.Subscription, doAck bool) error {
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
		if doAck {
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

func createTopicUrl(topic string) string {
	return "mem://topicA"
}
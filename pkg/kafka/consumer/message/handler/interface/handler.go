package kafkamessagehandlerinterface

import (
	"github.com/Shopify/sarama"
)

type ConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	// Messages returns messages chan.
	Messages() <-chan *kafkaconsumermessage.Message
	Close()
}

package kafkamessagehandlerinterface

import (
	kafkaconsumermessage "github.com/Borislavv/go-kafka/pkg/kafka/consumer/message"
	"github.com/Shopify/sarama"
)

type ConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	// Messages returns messages chan.
	Messages() <-chan *kafkaconsumermessage.Message
	Close()
}

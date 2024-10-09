package kafkaconsumerinterface

import (
	"context"
	kafkaconsumermessage "github.com/Borislavv/go-kafka/pkg/kafka/consumer/message"
)

type Consumer interface {
	Consume(ctx context.Context, topics []string) <-chan *kafkaconsumermessage.Message
	Close() error
}

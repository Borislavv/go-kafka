package kafkainterface

import (
	kafkaconsumerinterface "github.com/Borislavv/go-kafka/pkg/kafka/consumer/interface"
	kafkaproducerinterface "github.com/Borislavv/go-kafka/pkg/kafka/producer/interface"
	"io"
)

type ProducerConsumer interface {
	kafkaproducerinterface.Producer
	kafkaconsumerinterface.Consumer
	io.Closer
}

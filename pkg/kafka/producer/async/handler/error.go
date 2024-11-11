package kafkaasyncproducerhandler

import "github.com/IBM/sarama"

type ErrorHandler = func(message *sarama.ProducerError)

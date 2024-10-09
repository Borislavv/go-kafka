package kafkaasyncproducerhandler

import "github.com/Shopify/sarama"

type ErrorHandler = func(message *sarama.ProducerError)

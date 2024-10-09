package kafkaasyncproducerhandler

import "github.com/Shopify/sarama"

type SuccessHandler = func(message *sarama.ProducerMessage)

package kafkaasyncproducerhandler

import "github.com/IBM/sarama"

type SuccessHandler = func(message *sarama.ProducerMessage)

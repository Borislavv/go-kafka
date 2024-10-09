package kafkaproducerinterface

type Producer interface {
	Produce(topic, value string) error
	Close() error
}

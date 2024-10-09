package kafkaproducer

type Type int

const (
	Sync Type = iota
	Async
)

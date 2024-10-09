package kafkaconfiginterface

import kafkaproducer "github.com/Borislavv/go-kafka/pkg/kafka/producer"

type Configurator interface {
	GetAddrs() []string
	GetTLSEnabled() bool
	GetSASLMechanism() string
	GetSASLUser() string
	GetSASLPassword() string
	GetGroup() string
	GetCertsDir() string
	GetProducerType() kafkaproducer.Type
}

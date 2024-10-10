package kafkaconfig

import (
	kafkaconfiginterface "github.com/Borislavv/go-kafka/pkg/kafka/config/interface"
	kafkaproducer "github.com/Borislavv/go-kafka/pkg/kafka/producer"
	"time"
)

var _ kafkaconfiginterface.Configurator = (*Kafka)(nil)

type Kafka struct {
	Addrs                []string           `envconfig:"KAFKA_ADDRS" default:"host.docker.internal:9094"`
	TLSEnabled           bool               `envconfig:"KAFKA_TLS_ENABLED" default:"false"`
	SASLMechanism        string             `envconfig:"KAFKA_SASL_MECHANISM" default:"PLAIN"` // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLUser             string             `envconfig:"KAFKA_SASL_USER" default:"seoteam"`
	SASLPassword         string             `envconfig:"KAFKA_SASL_PASSWORD" default:"seoteam"`
	Group                string             `envconfig:"KAFKA_GROUP" default:"local-seo-gw-control-plane"`
	CertsDir             string             `envconfig:"KAFKA_CERTS_DIR" default:"certs"`
	ConsumeRetryInternal time.Duration      `envconfig:"KAFKA_CONSUME_RETRY_INTERNAL" default:"5s"`
	ProducerType         kafkaproducer.Type `envconfig:"KAFKA_PRODUCER_TYPE" default:"1"`
}

func (c Kafka) GetAddrs() []string {
	return c.Addrs
}

func (c Kafka) GetTLSEnabled() bool {
	return c.TLSEnabled
}

func (c Kafka) GetSASLMechanism() string {
	return c.SASLMechanism
}

func (c Kafka) GetSASLUser() string {
	return c.SASLUser
}

func (c Kafka) GetSASLPassword() string {
	return c.SASLPassword
}

func (c Kafka) GetGroup() string {
	return c.Group
}

func (c Kafka) GetCertsDir() string {
	return c.CertsDir
}

func (c Kafka) GetConsumeRetryInterval() time.Duration {
	return c.ConsumeRetryInternal
}

func (c Kafka) GetProducerType() kafkaproducer.Type {
	return c.ProducerType
}

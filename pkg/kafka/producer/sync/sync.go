package kafkasyncproducer

import (
	"context"
	"errors"
	kafkaconfiginterface "github.com/Borislavv/go-kafka/pkg/kafka/config/interface"
	kafkaproducerinterface "github.com/Borislavv/go-kafka/pkg/kafka/producer/interface"
	"github.com/Borislavv/go-logger/pkg/logger"
	"github.com/IBM/sarama"
)

var _ kafkaproducerinterface.Producer = (*Sync)(nil)

type Sync struct {
	logger         logger.Logger
	saramaProducer sarama.SyncProducer
}

func New(
	ctx context.Context,
	config *sarama.Config,
	cfg kafkaconfiginterface.Configurator,
	lgr logger.Logger,
) (*Sync, error) {
	config.Producer.Retry.Max = 3
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(cfg.GetAddrs(), config)
	if err != nil {
		return nil, lgr.Error(ctx, errors.New("failed to init new async producer"), logger.Fields{
			"err": err.Error(),
		})
	}

	return &Sync{
		logger:         lgr,
		saramaProducer: p,
	}, nil
}

func (p *Sync) Produce(topic, value string) error {
	if topic == "" {
		return errors.New("topic is required")
	}

	_, _, err := p.saramaProducer.SendMessage(
		&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(value),
		},
	)

	return err
}

func (p *Sync) Close() error {
	return p.saramaProducer.Close()
}

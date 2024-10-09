package kafka

import (
	"context"
	"embed"
	"errors"
	"fmt"
	kafkaconfiginterface "github.com/Borislavv/go-kafka/pkg/kafka/config/interface"
	kafkaconsumer "github.com/Borislavv/go-kafka/pkg/kafka/consumer"
	kafkaconsumerinterface "github.com/Borislavv/go-kafka/pkg/kafka/consumer/interface"
	kafkainterface "github.com/Borislavv/go-kafka/pkg/kafka/interface"
	kafkaproducer "github.com/Borislavv/go-kafka/pkg/kafka/producer"
	kafkaasyncproducer "github.com/Borislavv/go-kafka/pkg/kafka/producer/async"
	kafkaproducerinterface "github.com/Borislavv/go-kafka/pkg/kafka/producer/interface"
	kafkasyncproducer "github.com/Borislavv/go-kafka/pkg/kafka/producer/sync"
	kafkasaramaconfig "github.com/Borislavv/go-kafka/pkg/kafka/sarama/config"
	"github.com/Borislavv/go-logger/pkg/logger"
)

var _ kafkainterface.ProducerConsumer = (*Kafka)(nil)

type Kafka struct {
	logger logger.Logger
	kafkaconsumerinterface.Consumer
	kafkaproducerinterface.Producer
}

func New(
	ctx context.Context,
	cfg kafkaconfiginterface.Configurator,
	lgr logger.Logger,
	certsFS ...embed.FS,
) (kafka *Kafka, err error) {
	config, err := kafkasaramaconfig.New(cfg, certsFS...)
	if err != nil {
		return nil, lgr.Error(ctx, errors.New("failed to init sarama config"), logger.Fields{
			"error": err.Error(),
		})
	}

	consumer, err := kafkaconsumer.New(ctx, config, cfg, lgr)
	if err != nil {
		return nil, lgr.Error(ctx, errors.New("failed to init consumer"), logger.Fields{
			"error": err.Error(),
		})
	}

	var producer kafkaproducerinterface.Producer
	if cfg.GetProducerType() == kafkaproducer.Sync {
		producer, err = kafkasyncproducer.New(ctx, config, cfg, lgr)
		if err != nil {
			return nil, lgr.Error(ctx, errors.New("failed to init sync producer"), logger.Fields{
				"error": err.Error(),
			})
		}
	} else {
		producer, err = kafkaasyncproducer.New(ctx, config, cfg, lgr, nil, nil)
		if err != nil {
			return nil, lgr.Error(ctx, errors.New("failed to init async producer"), logger.Fields{
				"error": err.Error(),
			})
		}
	}

	return &Kafka{
		logger:   lgr,
		Consumer: consumer,
		Producer: producer,
	}, nil
}

func (k *Kafka) Close() error {
	cerr := k.Consumer.Close()
	perr := k.Producer.Close()
	if cerr != nil && perr != nil {
		return fmt.Errorf("consumer: %v, producer: %v", cerr.Error(), perr.Error())
	} else if cerr != nil || perr != nil {
		if cerr != nil {
			return fmt.Errorf("consumer: %v", cerr.Error())
		} else {
			return fmt.Errorf("producer: %v", perr.Error())
		}
	}
	return nil
}

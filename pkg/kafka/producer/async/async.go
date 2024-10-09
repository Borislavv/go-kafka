package kafkaasyncproducer

import (
	"context"
	"errors"
	kafkaconfiginterface "github.com/Borislavv/go-kafka/pkg/kafka/config/interface"
	kafkaasyncproducerhandler "github.com/Borislavv/go-kafka/pkg/kafka/producer/async/handler"
	kafkaproducerinterface "github.com/Borislavv/go-kafka/pkg/kafka/producer/interface"
	"github.com/Borislavv/go-logger/pkg/logger"
	"github.com/Shopify/sarama"
)

var _ kafkaproducerinterface.Producer = (*Async)(nil)

type Async struct {
	logger         logger.Logger
	saramaProducer sarama.AsyncProducer
	errorHandler   kafkaasyncproducerhandler.ErrorHandler
	successHandler kafkaasyncproducerhandler.SuccessHandler
}

func New(
	ctx context.Context,
	config *sarama.Config,
	cfg kafkaconfiginterface.Configurator,
	lgr logger.Logger,
	errorHandler kafkaasyncproducerhandler.ErrorHandler,
	successHandler kafkaasyncproducerhandler.SuccessHandler,
) (*Async, error) {
	config.Producer.Retry.Max = 3
	config.Producer.RequiredAcks = sarama.WaitForAll

	p, err := sarama.NewAsyncProducer(cfg.GetAddrs(), config)
	if err != nil {
		return nil, lgr.Error(ctx, errors.New("failed to init new async producer"), logger.Fields{
			"err": err.Error(),
		})
	}

	if errorHandler != nil {
		config.Producer.Return.Errors = true

		go func() {
			for e := range p.Errors() {
				errorHandler(e)
			}
		}()
	}

	if successHandler != nil {
		config.Producer.Return.Successes = true

		go func() {
			for v := range p.Successes() {
				successHandler(v)
			}
		}()
	}

	return &Async{
		logger:         lgr,
		saramaProducer: p,
		errorHandler:   errorHandler,
		successHandler: successHandler,
	}, nil
}

func (p *Async) Produce(topic, value string) error {
	if topic == "" {
		return errors.New("topic is required")
	}

	p.saramaProducer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
	}

	return nil
}

func (p *Async) Close() error {
	return p.saramaProducer.Close()
}

package kaƒkaconsumer

import (
	"context"
	"errors"
	kafkaconfiginterface "github.com/Borislavv/go-kafka/pkg/kafka/config/interface"
	kafkaconsumerinterface "github.com/Borislavv/go-kafka/pkg/kafka/consumer/interface"
	kafkaconsumermessage "github.com/Borislavv/go-kafka/pkg/kafka/consumer/message"
	kafkamessagehandler "github.com/Borislavv/go-kafka/pkg/kafka/consumer/message/handler"
	kafkamessagehandlerinterface "github.com/Borislavv/go-kafka/pkg/kafka/consumer/message/handler/interface"
	"github.com/Borislavv/go-logger/pkg/logger"
	"github.com/IBM/sarama"
	"hash/fnv"
	"slices"
	"strings"
	"sync"
	"time"
)

var _ kafkaconsumerinterface.Consumer = (*Consumer)(nil)

type Consumer struct {
	mu            *sync.Mutex
	wg            *sync.WaitGroup
	logger        logger.Logger
	cfg           kafkaconfiginterface.Configurator
	consumerGroup sarama.ConsumerGroup
	consumersMap  map[string]kafkamessagehandlerinterface.ConsumerGroupHandler
}

func New(
	ctx context.Context,
	config *sarama.Config,
	kafkaCfg kafkaconfiginterface.Configurator,
	lgr logger.Logger,
) (*Consumer, error) {
	consumerGroup, err := sarama.NewConsumerGroup(kafkaCfg.GetAddrs(), kafkaCfg.GetGroup(), config)
	if err != nil {
		return nil, lgr.Error(ctx, errors.New("failed to create consumer group"), logger.Fields{
			"error": err.Error(),
		})
	}

	return &Consumer{
		mu:            &sync.Mutex{},
		wg:            &sync.WaitGroup{},
		logger:        lgr,
		cfg:           kafkaCfg,
		consumerGroup: consumerGroup,
		consumersMap:  make(map[string]kafkamessagehandlerinterface.ConsumerGroupHandler, 1),
	}, nil
}

func (c *Consumer) Consume(ctx context.Context, topics []string) <-chan *kafkaconsumermessage.Message {
	hash := c.getConsumerHash(topics)

	if consumer, found := c.getConsumer(hash); found {
		return consumer.Messages()
	}

	consumer := kafkamessagehandler.NewConsumerGroupHandler()
	c.setConsumer(hash, consumer)

	c.wg.Add(1)
	go func() {
		defer func() {
			c.clsConsumer(hash, consumer)
			c.wg.Done()
		}()

		for {
			if err := c.consumerGroup.Consume(ctx, topics, consumer); err != nil {
				if ctx.Err() != nil || errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}

				c.logger.ErrorMsg(ctx, "kafka consumer error", logger.Fields{
					"err":    err.Error(),
					"topics": topics,
				})

				select {
				case <-ctx.Done():
					return
				default:
					c.awaitRetry(ctx)
				}
			}
		}
	}()

	return consumer.Messages()
}

func (c *Consumer) Close() error {
	c.wg.Wait()
	return c.consumerGroup.Close()
}

func (c *Consumer) getConsumerHash(topics []string) string {
	cTopics := make([]string, 0, len(topics))
	copy(cTopics, topics)
	slices.Sort(cTopics)

	uniqStr := strings.Join(cTopics, "_")

	hasher := fnv.New64()
	_, _ = hasher.Write([]byte(uniqStr))

	return string(hasher.Sum(nil))
}

func (c *Consumer) getConsumer(hash string) (consumer kafkamessagehandlerinterface.ConsumerGroupHandler, found bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.consumersMap[hash]
	return v, ok
}

func (c *Consumer) setConsumer(hash string, consumer kafkamessagehandlerinterface.ConsumerGroupHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumersMap[hash] = consumer
}

func (c *Consumer) clsConsumer(hash string, consumer kafkamessagehandlerinterface.ConsumerGroupHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.consumersMap, hash)
	consumer.Close()
}

func (c *Consumer) awaitRetry(ctx context.Context) {
	t := time.NewTimer(c.cfg.GetConsumeRetryInterval())
	defer t.Stop()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

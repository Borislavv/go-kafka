package kafkamessagehandler

import (
	"github.com/Borislavv/go-kafka/pkg/kafka/consumer/message"
)

var _ kafkamessagehandlerinterface.ConsumerGroupHandler = (*ConsumerGroupHandler)(nil)

type ConsumerGroupHandler struct {
	msgsCh chan *kafkaconsumermessage.Message
}

func NewConsumerGroupHandler() *ConsumerGroupHandler {
	return &ConsumerGroupHandler{msgsCh: make(chan *kafkaconsumermessage.Message)}
}

func (c *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			headers := make([]*kafkaconsumermessage.RecordHeader, 0, len(msg.Headers))

			for _, header := range msg.Headers {
				headers = append(headers, &kafkaconsumermessage.RecordHeader{
					Key:   header.Key,
					Value: header.Value,
				})
			}

			c.msgsCh <- &kafkaconsumermessage.Message{
				Headers:        headers,
				Timestamp:      msg.Timestamp,
				BlockTimestamp: msg.BlockTimestamp,
				Key:            msg.Key,
				Value:          msg.Value,
				Topic:          msg.Topic,
				Partition:      msg.Partition,
				Offset:         msg.Offset,
			}

			session.MarkMessage(msg, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *ConsumerGroupHandler) Messages() <-chan *kafkaconsumermessage.Message {
	return c.msgsCh
}

func (c *ConsumerGroupHandler) Close() {
	close(c.msgsCh)
}

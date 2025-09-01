package messagev2

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var producer *kafka.Writer

func InitProducer(brokers []string) {
	producer = &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  "im.msg.route",
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           10 * time.Second,
		ReadTimeout:            10 * time.Second,
		RequiredAcks:           kafka.RequireAll, // 确保所有副本确认
		AllowAutoTopicCreation: false,
	}
}

func ProduceMessage(topic, key string, value []byte) error {
	msg := kafka.Message{
		Key:   []byte(key), // 可用于 Kafka 分区（如按 to 分区）
		Value: value,
		Time:  time.Now(),
	}

	err := producer.WriteMessages(context.Background(), msg)
	if err != nil {
		zap.L().Error("Kafka write error", zap.Error(err))
	}
	return err
}

func StartConsumer(gatewayID string) { // 传入网关唯一标识
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "im.msg.route",
		GroupID:  "gateway-consumer-" + gatewayID, // 每个网关独立 group
		MinBytes: 10e3,
		MaxBytes: 10e6,
		// ✅ 提交 offset
		CommitInterval: 1 * time.Second, // 自动提交
	})

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			zap.S().Error("Kafka consume error", zap.Error(err))
			continue
		}

		var m struct{ To, Content string }
		if err := json.Unmarshal(msg.Value, &m); err != nil {
			zap.S().Warn("JSON unmarshal failed", zap.Error(err))
			continue
		}

		// 判断是否本网关用户
		if client, ok := GetClient(m.To); ok {
			select {
			case client.Send <- msg.Value:
			default:
				zap.S().Warn("Client buffer full", zap.String("user", m.To))
			}
		}
	}
}

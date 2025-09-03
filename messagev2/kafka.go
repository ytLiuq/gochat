package messagev2

import (
	"HiChat/global"
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func ProduceMessage(topic, key string, value []byte) error {
	msg := kafka.Message{
		Key:   []byte(key), // 可用于 Kafka 分区（如按 to 分区）
		Value: value,
		Time:  time.Now(),
	}

	err := global.Producer.WriteMessages(context.Background(), msg)
	if err != nil {
		zap.L().Error("Kafka write error", zap.Error(err))
	}
	return err
}

// messagev2/consumer.go

func StartConsumer(ctx context.Context, gw *Gateway) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"}, // 建议从配置传入
		Topic:          "im.msg.route",
		GroupID:        "gateway-group", // ✅ 所有网关共享同一个 GroupID
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 1 * time.Second,
	})
	defer reader.Close()

	zap.L().Info("Kafka consumer started", zap.String("gateway", gw.ID))

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			zap.L().Error("Kafka consume error", zap.Error(err))
			continue
		}

		var m struct {
			To      string `json:"To"`      // 注意字段名大小写（必须与发送端一致）
			Content string `json:"Content"` // 可选：用于日志
		}
		if err := json.Unmarshal(msg.Value, &m); err != nil {
			zap.L().Warn("JSON unmarshal failed", zap.Error(err), zap.ByteString("value", msg.Value))
			continue
		}

		// ✅ 使用 gw.GetClient(m.To) 查询本网关是否连接了该用户
		if client, ok := gw.GetClient(m.To); ok {
			select {
			case client.Send <- msg.Value:
				zap.L().Debug("Message delivered to local client",
					zap.String("user", m.To),
					zap.String("gateway", gw.ID),
				)
			default:
				zap.L().Warn("Client buffer full, message dropped",
					zap.String("user", m.To),
					zap.String("gateway", gw.ID),
				)
			}
		} else {
			// 用户不在此网关（正常情况：在其他网关）
			zap.L().Debug("User not connected to this gateway, skipping",
				zap.String("user", m.To),
				zap.String("gateway", gw.ID),
			)
		}
	}
}

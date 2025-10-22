package messagev2

import (
	"HiChat/global"
	"context"
	"encoding/json"
	"time"

	"fmt"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// GatewayIDToPartition 返回网关对应的 partition（确定性映射）
func GatewayIDToPartition(gatewayID string) (int, error) {
	switch gatewayID {
	case "gateway-1":
		return 0, nil
	case "gateway-2":
		return 1, nil
	case "gateway-3":
		return 2, nil
	default:
		return -1, fmt.Errorf("unknown gateway ID: %s", gatewayID)
	}
}

// PartitionToGatewayID 反向映射（用于日志或校验）
func PartitionToGatewayID(partition int) (string, error) {
	switch partition {
	case 0:
		return "gateway-1", nil
	case 1:
		return "gateway-2", nil
	case 2:
		return "gateway-3", nil
	default:
		return "", fmt.Errorf("invalid partition: %d", partition)
	}
}

func ProduceMessage(targetGatewayID string, value []byte) error {
	partition, err := GatewayIDToPartition(targetGatewayID)
	if err != nil {
		zap.S().Error("Invalid target gateway", zap.String("gateway", targetGatewayID), zap.Error(err))
		return err
	}

	msg := kafka.Message{
		Value:     value,
		Time:      time.Now(),
		Partition: partition, // 👈 关键：显式指定 partition
		// 注意：不再设置 Key，因为 partition 已确定
	}

	// 使用全局 Producer（确保它没有设置 Balancer 干扰）
	err = global.Producer.WriteMessages(context.Background(), msg)
	if err != nil {
		zap.S().Error("Kafka produce failed",
			zap.String("target_gateway", targetGatewayID),
			zap.Int("partition", partition),
			zap.Error(err))
	}
	return err
}

// messagev2/consumer.go
func (g *Gateway) StartConsumerForPartition(ctx context.Context) {
	partition, err := GatewayIDToPartition(g.ID)
	if err != nil {
		zap.S().Fatal("Gateway has invalid ID", zap.String("gateway", g.ID), zap.Error(err))
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "im.msg.route",
		Partition: partition, // 👈 只读这个 partition
		MinBytes:  10e3,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	zap.S().Info("Kafka consumer started",
		zap.String("gateway", g.ID),
		zap.Int("partition", partition))

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			zap.S().Error("Kafka read error", zap.Error(err))
			continue
		}

		// 解析消息
		var m Message
		if err := json.Unmarshal(msg.Value, &m); err != nil {
			zap.S().Warn("Unmarshal failed", zap.Error(err))
			continue
		}

		// ✅ 信任 partition 隔离：这个 partition 的所有消息都属于本网关
		// 但仍建议做轻量校验（防御性编程）
		expectedGateway, online, _ := GetUserGateway(m.To)
		if !online || expectedGateway != g.ID {
			zap.S().Warn("Message arrived but user not online or in wrong gateway",
				zap.String("user", m.To),
				zap.String("expected_gateway", expectedGateway),
				zap.Bool("online", online))
			// 可选：存离线？但通常发送方已处理
			continue
		}

		if client, ok := g.GetClient(m.To); ok {
			select {
			case client.Send <- msg.Value:
			default:
				zap.S().Warn("Client send buffer full", zap.String("user", m.To))
			}
		}
	}
}

package initialize

import (
	"HiChat/global"
	"time"

	"github.com/segmentio/kafka-go"
)

func InitProducer() {
	global.Producer = &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "im.msg.route",
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           10 * time.Second,
		ReadTimeout:            10 * time.Second,
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,

		// 🔧 增加重试
		MaxAttempts: 5,

		// 🔧 批量发送（提升吞吐）
		BatchTimeout: 100 * time.Millisecond,
		BatchSize:    100,
		BatchBytes:   10 * 1024 * 1024, // 10MB

		// 🔧 启用压缩（减少网络）
		Compression: kafka.Snappy,
	}
}

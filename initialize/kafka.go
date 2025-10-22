package initialize

import (
	"HiChat/global"

	"github.com/segmentio/kafka-go"
)

func InitProducer() {
	global.Producer = &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "im.msg.route",
		// 不设置 Balancer！否则会覆盖 Partition 字段
		RequiredAcks: kafka.RequireAll,
	}
}

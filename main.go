package main

import (
	_ "HiChat/docs"
	"HiChat/global"
	"HiChat/initialize"
	"HiChat/messagesave"
	"HiChat/messagev2"
	"HiChat/router"
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	//swaggerFiles "github.com/swaggo/files"
	//ginSwagger "github.com/swaggo/gin-swagger"
)

// @title 这是一个测试文档
// @version 1.0
// @description HiCha聊天服务
// @host 127.0.0.1:8000
// @BasePath /v1
func main() {
	//初始化日志
	initialize.InitLogger()
	//初始化配置
	initialize.InitConfig()
	//初始化数据库
	initialize.InitDB()
	initialize.InitRedis()
	initialize.InitProducer()

	gateways := []*messagev2.Gateway{
		messagev2.NewGateway("gateway-1", 8081),
		messagev2.NewGateway("gateway-2", 8082),
		messagev2.NewGateway("gateway-2", 8083),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动所有网关（每个监听不同端口）
	for _, g := range gateways {
		g.Start(ctx)
	}

	// 启动消息发送 API（主服务端口）
	go func() {
		r := router.Router()
		port := global.ServiceConfig.Port // 如 8000
		if err := r.Run(fmt.Sprintf(":%d", port)); err != nil {
			zap.S().Error("API server failed", zap.Error(err))
		}
	}()

	go messagesave.StartArchiveJob(5 * time.Minute)

}

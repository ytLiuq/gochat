package messagev2

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

type Message struct {
	MsgID     string // 可用雪花算法生成唯一 ID
	From      string
	To        string
	Content   string
	Timestamp int64 // 或者 time.Time，看你存什么
}

// ReadPump —— 读取消息
func (c *Client) ReadPump() {
	defer func() {
		RemoveClient(c.UserID)
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}

		// 处理收到的消息（如：转发给 Kafka 或本地投递）
		go c.handleMessage(message)
	}
}

// WritePump —— 发送消息
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// 处理客户端发来的消息
func (c *Client) handleMessage(message []byte) {
	var msg struct {
		To      string `json:"to"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(message, &msg); err != nil {
		return
	}

	// 调用消息路由服务（判断本地 or Kafka）
	SendMessage(c.UserID, msg.To, msg.Content)
}

const KafkaTopic = "im.msg.route"

// SendMessage 发送消息主逻辑
func SendMessage(from, to, content string) error {
	// 1. 构造消息体
	msg := Message{
		MsgID:     generateMsgID(), // 可用雪花算法生成唯一 ID
		From:      from,
		To:        to,
		Content:   content,
		Timestamp: time.Now().Unix(),
	}

	// 2. 序列化
	value, err := json.Marshal(msg)
	if err != nil {
		zap.S().Error("Message marshal failed", zap.Error(err))
		return err
	}

	// 3. 查询目标用户所在网关
	targetGateway, online, err := GetUserGateway(to)
	if err != nil {
		zap.S().Error("Redis query failed", zap.String("to", to), zap.Error(err))
		return err
	}

	if !online {
		// 用户离线，可调用离线消息服务（本期暂不实现）
		zap.S().Info("User offline, skipping", zap.String("to", to))
		return nil // 或保存离线消息
	}

	// 4. 判断是否在本网关
	if localClient, ok := GetClient(to); ok {
		select {
		case localClient.Send <- value:
			zap.S().Debug("Message delivered locally", zap.String("from", from), zap.String("to", to))
		default:
			// Send channel 满了，客户端可能卡顿
			zap.S().Warn("Client send buffer full, message dropped", zap.String("user", to))
			// 可考虑关闭连接或转离线
		}
		return nil
	}

	// 🚀 发送到 Kafka，跨网关路由
	err = ProduceMessage(KafkaTopic, to, value)
	if err != nil {
		zap.S().Error("Kafka produce failed", zap.String("to", to), zap.Error(err))
		return err
	}
	zap.S().Debug("Message routed via Kafka", zap.String("from", from), zap.String("to", to), zap.String("target_gateway", targetGateway))

	return nil
}

// generateMsgID 生成唯一消息 ID（示例：时间戳 + 随机数）
// 建议使用雪花算法（snowflake）替代
func generateMsgID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(10000))
}

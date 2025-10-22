package messagev2

import (
	"HiChat/dao"
	"HiChat/global"
	"HiChat/messagesave"
	"HiChat/models"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"strconv"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 30 * time.Second    // 从 60s 改为 30s
	pingPeriod     = (pongWait * 9) / 10 // = 27s
	maxMessageSize = 512
)

type Message struct {
	MsgID     string // 可用雪花算法生成唯一 ID
	ChatType  string
	From      string
	To        string
	Content   string
	Timestamp time.Time // 或者 time.Time，看你存什么
}

// ReadPump —— 读取消息
func (c *Client) ReadPump() {
	defer func() {
		gateway, ok := GetGatewayByID(c.Gateway)
		if !ok {
			zap.L().Warn("Gateway not found during disconnect",
				zap.String("user_id", c.UserID),
				zap.String("gateway_id", c.Gateway))
			// 继续关闭连接
		} else {
			gateway.RemoveClient(c.UserID)
		}
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(512)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(appData string) error {
		// 1. 更新读超时（原有逻辑）
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))

		// 2. 刷新 Redis 中的在线状态 TTL（新增逻辑）
		ctx := context.Background()
		key := "user_conn:" + c.UserID
		if err := global.RedisDB.Expire(ctx, key, ConnTTL).Err(); err != nil {
			zap.S().Warn("Failed to refresh user online TTL in Redis",
				zap.String("user_id", c.UserID),
				zap.Error(err))
		}
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}

		// 处理收到的消息
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
		To          string `json:"to"`
		Chattype    string `json:"chat_type"`
		Content     string `json:"content"`
		ClientMsgID string `json:"client_msg_id"`
	}
	if err := json.Unmarshal(message, &msg); err != nil {
		return
	}

	gateway, ok := GetGatewayByID(c.Gateway)
	if !ok {
		zap.S().Error("Gateway not found for client", zap.String("gateway", c.Gateway))
		return
	}

	switch msg.Chattype {
	case "group":
		gateway.SendGroupMessage(c.UserID, msg.To, msg.Content, msg.ClientMsgID)
	case "private", "":
		gateway.SendMessage(c.UserID, msg.To, msg.Content, msg.ClientMsgID)
	default:
		zap.S().Warn("Unsupported chat type", zap.String("type", msg.Chattype))
	}
}

const KafkaTopic = "im.msg.route"

func (g *Gateway) SendGroupMessage(from, groupID, content, clientMsgID string) error {
	// 1. 校验权限
	inGroup, err := dao.IsUserInGroup(groupID, from)
	if err != nil {
		zap.S().Error("find user group faild", zap.Error(err))
		return err
	}
	if !inGroup {
		return errors.New("user not in group")
	}

	// 2. 使用统一 conversation_id
	convID := GetGroupConvID(groupID) // "group:123"

	// 3. 构造消息并保存一次
	msgID := generateMsgID()
	msg := Message{
		MsgID:     msgID,
		ChatType:  "group",
		From:      from,
		To:        groupID,
		Content:   content,
		Timestamp: time.Now(),
	}

	value, _ := json.Marshal(msg)

	// ✅ 只保存一次
	messagesave.Save(context.Background(), &messagesave.Message{
		ID:             msgID,
		ConversationID: convID,
		SenderID:       from,
		Content:        []byte(content),
		MsgType:        "text",
		Timestamp:      msg.Timestamp,
		ClientMsgID:    clientMsgID,
	})

	// 4. 获取所有成员并广播
	uintid, err := strconv.ParseUint(groupID, 10, 64)
	if err != nil {
		zap.S().Error("groupid parsed faild", zap.Error(err))
		return err
	}
	members, _ := models.FindUsers(uint(uintid))
	for _, memberID := range *members {
		idstring := strconv.FormatUint(uint64(memberID), 10)
		if idstring == from {
			continue
		}
		// 发送给 memberID（走本地 or Kafka）
		g.sendToMember(idstring, value)
	}

	return nil
}

// sendToMember 将消息发送给指定用户（群聊或私聊复用）
func (g *Gateway) sendToMember(userID string, message []byte) {
	// 1. 查询用户所在网关，判断是否在线
	targetGateway, online, err := GetUserGateway(userID)
	if err != nil {
		zap.S().Error("Failed to query user gateway from Redis",
			zap.String("user_id", userID),
			zap.Error(err))
		return
	}

	if !online {
		// 用户离线，保存到其离线队列（以 userID 为 key）
		if err := saveOfflineMessage(userID, message); err != nil {
			zap.S().Error("Failed to save offline message",
				zap.String("to", userID),
				zap.Error(err))
			// 不阻塞主流程
		} else {
			zap.S().Info("Message saved to offline queue",
				zap.String("to", userID))
		}
		return
	}

	// 2. 如果目标用户在本网关，尝试本地投递
	if targetGateway == g.ID {
		if client, ok := g.GetClient(userID); ok {
			select {
			case client.Send <- message:
				zap.S().Debug("Message delivered locally",
					zap.String("user_id", userID),
					zap.String("gateway", g.ID))
			default:
				zap.S().Warn("Client send buffer full, dropping message",
					zap.String("user_id", userID))
				// 可选：转为离线？但通常说明客户端异常，可记录或断开
			}
		} else {
			// Redis 认为在线，但本地没找到：状态不一致（可能刚断开）
			zap.S().Warn("User marked online in Redis but not found in gateway",
				zap.String("user_id", userID),
				zap.String("gateway", g.ID))
			// 可选：清理 Redis？或忽略（下次心跳会过期）
		}
		return
	}

	// 3. 目标用户在其他网关，通过 Kafka 路由
	if err := ProduceMessage(targetGateway, message); err != nil {
		zap.S().Error("Failed to route message via Kafka",
			zap.String("user_id", userID),
			zap.String("target_gateway", targetGateway),
			zap.Error(err))
		return
	}

	zap.S().Debug("Message routed via Kafka",
		zap.String("user_id", userID),
		zap.String("target_gateway", targetGateway))
}

// SendMessage 发送消息主逻辑
func (g *Gateway) SendMessage(from, to, content, clientMsgID string) error {
	// 1. 构造消息体
	msg := Message{
		MsgID:     generateMsgID(), // 可用雪花算法生成唯一 ID
		From:      from,
		To:        to,
		Content:   content,
		Timestamp: time.Now(),
	}

	// 2. 序列化
	value, err := json.Marshal(msg)
	if err != nil {
		zap.S().Error("Message marshal failed", zap.Error(err))
		return err
	}

	err = messagesave.Save(context.Background(), &messagesave.Message{
		ID:             msg.MsgID,
		ConversationID: GetConversationID(from, to), // 见下方辅助函数
		SenderID:       from,
		Content:        []byte(content), // 或封装更复杂的结构
		MsgType:        "text",          // 可扩展
		Timestamp:      msg.Timestamp,
		ClientMsgID:    clientMsgID, // 如果客户端传了去重ID，可从 handleMessage 解析传入
	})
	if err != nil {
		zap.S().Error("Failed to save message", zap.String("msg_id", msg.MsgID), zap.Error(err))
		// ❗注意：这里不 return！消息可继续发送，只是未持久化
		// 你可以选择：继续发送 or 阻止发送？
		// 建议：继续发送，但记录日志，后续重试补录
	}

	// 3. 查询目标用户所在网关
	targetGatewayID, online, err := GetUserGateway(to)
	if err != nil {
		zap.S().Error("Redis query failed", zap.String("to", to), zap.Error(err))
		return err
	}

	if !online {
		err = saveOfflineMessage(to, value)
		if err != nil {
			zap.S().Error("Failed to save offline message", zap.String("to", to), zap.String("msg_id", msg.MsgID), zap.Error(err))
			// 可以选择忽略或返回错误
			return nil // 通常离线消息失败不应阻塞主流程
		}
		zap.S().Info("Message saved to offline queue", zap.String("to", to), zap.String("msg_id", msg.MsgID))
		return nil
	}

	// 4. 判断是否在本网关
	if targetGatewayID == g.ID {
		if client, ok := g.GetClient(to); ok {
			select {
			case client.Send <- value:
				zap.S().Debug("Delivered locally", zap.String("to", to))
			default:
				zap.S().Warn("Send buffer full", zap.String("to", to))
			}
			return nil
		}
		// 如果 Redis 说在本网关，但实际没找到，可能是状态不一致
		zap.S().Warn("User claimed online in this gateway but not found", zap.String("user", to))
	}

	// 🚀 发送到 Kafka，跨网关路由
	err = ProduceMessage(targetGatewayID, value)
	if err != nil {
		zap.S().Error("Kafka produce failed", zap.String("to", to), zap.Error(err))
		return err
	}
	zap.S().Debug("Message routed via Kafka", zap.String("from", from), zap.String("to", to))

	return nil
}

func saveOfflineMessage(userID string, messageData []byte) error {
	ctx := context.Background()
	key := fmt.Sprintf("offline:messages:%s", userID)

	// 设置过期时间（例如 7 天）
	_, err := global.RedisDB.RPush(ctx, key, messageData).Result()
	if err != nil {
		return err
	}

	// 设置 key 过期时间（如果之前没有设置）
	_, err = global.RedisDB.Expire(ctx, key, 7*24*time.Hour).Result()
	return err
}

func GetConversationID(uid1, uid2 string) string {
	if uid1 > uid2 {
		uid1, uid2 = uid2, uid1
	}
	return "user:" + uid1 + ":" + uid2
}

func GetGroupConvID(groupID string) string {
	return "group:" + groupID
}

// generateMsgID 生成唯一消息 ID（示例：时间戳 + 随机数）
// 建议使用雪花算法（snowflake）替代
func generateMsgID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(10000))
}

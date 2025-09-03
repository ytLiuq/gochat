package messagev2

import (
	"HiChat/dao"
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
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
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

func GetLocalGateway() *Gateway {
	return LocalGateway
}

// ReadPump —— 读取消息
func (c *Client) ReadPump() {
	defer func() {
		// 从当前网关移除
		gateway := GetLocalGateway() // 获取当前网关实例
		if gateway != nil {
			gateway.RemoveClient(c.UserID)
		}
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(512)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
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

	switch msg.Chattype {
	case "group":
		SendGroupMessage(c.UserID, msg.To, msg.Content, msg.ClientMsgID)
	case "private", "":
		SendMessage(c.UserID, msg.To, msg.Content, msg.ClientMsgID)
	default:
		zap.S().Warn("Unsupported chat type", zap.String("type", msg.Chattype))
	}
}

const KafkaTopic = "im.msg.route"

func SendGroupMessage(from, groupID, content, clientMsgID string) error {
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
		sendToMember(idstring, value)
	}

	return nil
}

// sendToMember 将群消息发送给指定成员
func sendToMember(userID string, message []byte) {
	// 1. 查询用户所在网关，判断是否在线
	targetGateway, online, err := GetUserGateway(userID)
	if err != nil {
		zap.S().Error("Failed to query user gateway from Redis",
			zap.String("user_id", userID),
			zap.Error(err))
		return
	}

	if !online {
		// 用户离线，可选择保存离线消息（本期可暂不处理）
		zap.S().Info("User is offline, skipping group message",
			zap.String("user_id", userID))
		return
	}

	// 2. 检查是否在本网关
	if localClient, ok := GetClient(userID); ok {
		select {
		case localClient.Send <- message:
			zap.S().Debug("Group message delivered locally",
				zap.String("user_id", userID))
		default:
			// Send channel 满载，客户端可能卡顿或未及时消费
			zap.S().Warn("Client send buffer full, dropping group message",
				zap.String("user_id", userID))
			// 可考虑关闭连接，或转为离线推送
		}
		return
	}

	// 3. 不在本网关，通过 Kafka 发送到目标网关
	err = ProduceMessage(KafkaTopic, userID, message)
	if err != nil {
		zap.S().Error("Failed to route group message via Kafka",
			zap.String("user_id", userID),
			zap.String("target_gateway", targetGateway),
			zap.Error(err))
		return
	}

	zap.S().Debug("Group message routed via Kafka",
		zap.String("user_id", userID),
		zap.String("target_gateway", targetGateway))
}

// SendMessage 发送消息主逻辑
func SendMessage(from, to, content, ClientMsgID string) error {
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
		ClientMsgID:    ClientMsgID, // 如果客户端传了去重ID，可从 handleMessage 解析传入
	})
	if err != nil {
		zap.S().Error("Failed to save message", zap.String("msg_id", msg.MsgID), zap.Error(err))
		// ❗注意：这里不 return！消息可继续发送，只是未持久化
		// 你可以选择：继续发送 or 阻止发送？
		// 建议：继续发送，但记录日志，后续重试补录
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

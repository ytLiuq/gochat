// messagev2/gateway.go

package messagev2

import (
	"HiChat/global"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 允许所有来源
	},
}

// Client 代表一个用户 WebSocket 连接
type Client struct {
	UserID  string
	Gateway string
	Conn    *websocket.Conn
	Send    chan []byte
}

// Gateway 代表一个网关节点（可运行多个实例）
type Gateway struct {
	ID      string
	Port    int
	Clients map[string]*Client // userID -> Client
	Mu      sync.RWMutex
}

var (
	gateways   = make(map[string]*Gateway)
	gatewaysMu sync.RWMutex
)

func RegisterGateway(g *Gateway) {
	gatewaysMu.Lock()
	defer gatewaysMu.Unlock()
	gateways[g.ID] = g
}

// GetGatewayByID 根据 ID 获取网关实例
func GetGatewayByID(gatewayID string) (*Gateway, bool) {
	gatewaysMu.RLock()
	defer gatewaysMu.RUnlock()
	g, ok := gateways[gatewayID]
	return g, ok
}

// NewGateway 创建新网关
func NewGateway(id string, port int) *Gateway {
	return &Gateway{
		ID:      id,
		Port:    port,
		Clients: make(map[string]*Client),
	}
}

// AddClient 注册客户端
func (g *Gateway) AddClient(client *Client) {
	g.Mu.Lock()
	g.Clients[client.UserID] = client
	g.Mu.Unlock()

	// 向 Redis 注册用户所在网关（用于路由）
	ctx := context.Background()
	err := global.RedisDB.Set(ctx, "user_conn:"+client.UserID, g.ID, 30*time.Second).Err()
	if err != nil {
		zap.S().Warn("Redis set failed", zap.String("user", client.UserID), zap.Error(err))
	}
	zap.S().Info("User connected", zap.String("user", client.UserID), zap.String("gateway", g.ID))
}

// RemoveClient 注销客户端
func (g *Gateway) RemoveClient(userID string) {
	g.Mu.Lock()
	delete(g.Clients, userID)
	g.Mu.Unlock()

	// 从 Redis 删除
	ctx := context.Background()
	global.RedisDB.Del(ctx, "user_conn:"+userID)
	zap.S().Info("User disconnected", zap.String("user", userID))
}
func GetClientByUserID(userID string) (*Client, bool) {
	// 1. 查 Redis：用户在哪个网关？
	gatewayID, online, err := GetUserGateway(userID)
	if !online || err != nil {
		return nil, false
	}

	// 2. 获取网关实例
	gateway, ok := GetGatewayByID(gatewayID)
	if !ok {
		return nil, false
	}

	// 3. 在网关中查找 Client
	return gateway.GetClient(userID)
}

// GetClient 获取本地客户端
func (g *Gateway) GetClient(userID string) (*Client, bool) {
	g.Mu.RLock()
	defer g.Mu.RUnlock()
	c, ok := g.Clients[userID]
	return c, ok
}

// HandleWebSocket 处理 WebSocket 连接（所有网关共用）
func (g *Gateway) HandleWebSocket(c *gin.Context) {
	userID := c.Query("userId")
	if userID == "" {
		c.JSON(400, gin.H{"error": "missing userId"})
		c.Abort()
		return
	}

	// 升级为 WebSocket 连接
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		zap.L().Error("WebSocket upgrade failed", zap.Error(err))
		return
	}

	client := &Client{
		UserID:  userID,
		Gateway: g.ID,
		Conn:    conn,
		Send:    make(chan []byte, 1000),
	}
	// 注册到本地
	g.AddClient(client)
	DeliverOfflineMessages(userID, client)

	// 启动读写协程
	go client.WritePump()
	go client.ReadPump()
}

// Start 启动网关服务（HTTP + Kafka 消费）
func (g *Gateway) Start(ctx context.Context) {
	// 创建独立的 Gin 引擎
	RegisterGateway(g)
	r := gin.New()

	// 中间件：日志和恢复
	r.Use(gin.Recovery())
	r.Use(gin.Logger())
	r.Use(func(c *gin.Context) {
		zap.L().Debug("HTTP Request", zap.String("path", c.Request.URL.Path))
		c.Next()
	})

	// 注册 WebSocket 路由
	r.GET("/ws", g.HandleWebSocket)
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":  "ok",
			"gateway": g.ID,
			"port":    g.Port,
		})
	})

	// 启动 HTTP 服务
	addr := fmt.Sprintf(":%d", g.Port)
	go func() {
		zap.L().Info("Gateway HTTP server starting",
			zap.String("addr", addr),
			zap.String("id", g.ID))
		if err := r.Run(addr); err != nil {
			zap.L().Error("HTTP server error", zap.Error(err))
		}
	}()

	// 启动 Kafka 消费者（所有网关共享 group，负载均衡）
	go StartConsumer(ctx, g)
}

func DeliverOfflineMessages(userID string, client *Client) {
	ctx := context.Background()
	key := fmt.Sprintf("offline:messages:%s", userID)

	// 获取所有离线消息
	values, err := global.RedisDB.LRange(ctx, key, 0, -1).Result()
	if err != nil || len(values) == 0 {
		return
	}

	// 逆序发送（最新的在后面？根据业务定），也可以正序
	for _, msgData := range values {
		select {
		case client.Send <- []byte(msgData):
			zap.S().Debug("Delivered offline message", zap.String("user", userID))
		default:
			zap.S().Warn("Client offline buffer full during delivery", zap.String("user", userID))
			// 可记录未送达，或断开连接``
			continue
		}
	}

	// 全部投递成功后清除离线消息
	global.RedisDB.Del(ctx, key)
}

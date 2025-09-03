// messagev2/gateway.go

package messagev2

import (
	"HiChat/global"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // 开发允许跨域
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
	// 全局持有当前网关实例（用于 SendMessage 调用）
	LocalGateway *Gateway
)

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

func GetClient(userID string) (*Client, bool) {
	if LocalGateway == nil {
		return nil, false
	}
	return LocalGateway.GetClient(userID)
}

// GetClient 获取本地客户端
func (g *Gateway) GetClient(userID string) (*Client, bool) {
	g.Mu.RLock()
	defer g.Mu.RUnlock()
	c, ok := g.Clients[userID]
	return c, ok
}

// HandleWebSocket 处理 WebSocket 连接（所有网关共用）
func (g *Gateway) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userId")
	if userID == "" {
		http.Error(w, "missing userId", http.StatusBadRequest)
		return
	}

	// 升级为 WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		zap.S().Error("Upgrade failed", zap.Error(err))
		return
	}

	client := &Client{
		UserID:  userID,
		Gateway: g.ID,
		Conn:    conn,
		Send:    make(chan []byte, 10),
	}

	// 注册到本地
	g.AddClient(client)

	// 启动读写协程
	go client.WritePump()
	go client.ReadPump()
}

// Start 启动网关服务（HTTP + Kafka 消费）
func (g *Gateway) Start(ctx context.Context) {
	// 设置 WebSocket 路由
	LocalGateway = g
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		g.HandleWebSocket(w, r)
	})

	// 启动 HTTP 服务
	addr := fmt.Sprintf(":%d", g.Port)
	go func() {
		zap.L().Info("Gateway HTTP server starting", zap.String("addr", addr), zap.String("id", g.ID))
		if err := http.ListenAndServe(addr, nil); err != nil {
			zap.L().Error("HTTP server error", zap.Error(err))
		}
	}()

	// 启动 Kafka 消费者（所有网关共享 group，负载均衡）
	go StartConsumer(ctx, g)
}

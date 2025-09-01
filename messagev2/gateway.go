package messagev2

import (
	"HiChat/global"
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // 生产环境应校验 Origin
}

type Client struct {
	GatewayID string
	UserID    string
	Conn      *websocket.Conn
	Send      chan []byte // 待发送的消息队列
	mutex     sync.Mutex
}

func NewClient(userID, gatewayID string, conn *websocket.Conn) *Client {
	return &Client{
		UserID:    userID,
		GatewayID: gatewayID,
		Conn:      conn,
		Send:      make(chan []byte, 100), // 缓冲 channel 防止阻塞
	}
}

var clientManager = struct {
	clients map[string]*Client // userID -> Client
	mutex   sync.RWMutex
}{
	clients: make(map[string]*Client),
}

func AddClient(client *Client) {
	clientManager.mutex.Lock()
	defer clientManager.mutex.Unlock()
	clientManager.clients[client.UserID] = client
}

func RemoveClient(userID string) {
	clientManager.mutex.Lock()
	defer clientManager.mutex.Unlock()
	delete(clientManager.clients, userID)
}

func GetClient(userID string) (*Client, bool) {
	clientManager.mutex.RLock()
	defer clientManager.mutex.RUnlock()
	client, exists := clientManager.clients[userID]
	return client, exists
}

func BroadcastToLocals(message []byte) {
	clientManager.mutex.RLock()
	defer clientManager.mutex.RUnlock()
	for _, client := range clientManager.clients {
		select {
		case client.Send <- message:
		default:
			// Send channel 满了，可能是客户端太慢，考虑断开
			RemoveClient(client.UserID)
			client.Conn.Close()
		}
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 1. 鉴权：从 query 或 header 获取 token
	userID := r.URL.Query().Get("userId")

	// 2. 升级为 WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		zap.S().Info("Upgrade failed: %v", err)
		return
	}

	client := NewClient(userID, "gateway-1:8080", conn)

	// 3. 注册到本地管理器
	AddClient(client)

	// 4. 向 Redis 注册连接关系（带 TTL）
	ctx := context.Background()
	err = global.RedisDB.Set(ctx, "user_conn:"+userID, "gateway-1:8080", 30*time.Second).Err()
	if err != nil {
		zap.S().Info("Redis set failed: %v", err)
	}

	// 5. 启动读写 goroutine
	go client.WritePump()
	go client.ReadPump()
}

// func main() {
//     // 初始化 Redis
//     if err := redis.Init(); err != nil {
//         log.Fatal("Redis init failed: ", err)
//     }

//     // 启动 Kafka 消费者（监听跨网关消息）
//	   kafka.InitProducer([]string{"localhost:9092"})
//     go kafka.StartConsumer()

//     // HTTP 路由
//     http.HandleFunc("/ws", handleWebSocket)
//     port := os.Getenv("PORT")
//     if port == "" {
//         port = "8080"
//     }

//     go func() {
//         log.Println("Gateway server started on :" + port)
//         if err := http.ListenAndServe(":"+port, nil); err != nil {
//             log.Fatal("Server failed: ", err)
//         }
//     }()

//     // 优雅关闭
//     c := make(chan os.Signal, 1)
//     signal.Notify(c, os.Interrupt)
//     <-c
//     log.Println("Shutting down...")
// }

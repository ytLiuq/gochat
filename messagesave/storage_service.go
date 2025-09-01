package messagesave

import (
	"HiChat/global"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// 假设你在项目中已经定义了这两个全局变量
// var redisdb *redis.Client
// var DB *gorm.DB

// StorageService 使用全局 redisdb 和 DB，无需传参
type StorageService struct {
	// 无需字段，直接使用全局实例
}

func NewStorageService() *StorageService {
	svc := &StorageService{}

	// 启动归档协程
	go svc.StartArchiveJob(5 * time.Minute)
	return svc
}

// Save：只写 Redis（热存储），归档任务负责落库
func (s *StorageService) Save(ctx context.Context, msg *Message) error {
	// 直接使用全局 redisdb
	if err := saveToRedis(ctx, msg); err != nil {
		zap.S().Info("Redis 写入失败: %v", err)
	}
	return nil
}

func saveToRedis(ctx context.Context, msg *Message) error {
	if msg.ID == "" || msg.ConversationID == "" || msg.Timestamp.IsZero() {
		return fmt.Errorf("消息缺少必要字段")
	}

	// 幂等控制
	if msg.ClientMsgID != "" {
		key := fmt.Sprintf("msgid:%s:%s", msg.ConversationID, msg.ClientMsgID)
		exists, err := global.RedisDB.Exists(ctx, key).Result()
		if err != nil {
			return err
		}
		if exists > 0 {
			return nil
		}
		global.RedisDB.SetEX(ctx, key, "1", 24*time.Hour)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("JSON 序列化失败: %v", err)
	}

	zsetKey := fmt.Sprintf("conv:msg:%s", msg.ConversationID)
	hashKey := fmt.Sprintf("msg:%s", msg.ID)
	score := float64(msg.Timestamp.UnixNano())

	pipe := global.RedisDB.TxPipeline()
	pipe.ZAdd(ctx, zsetKey, &redis.Z{Score: score, Member: msg.ID})
	pipe.HSet(ctx, hashKey, "data", data)
	pipe.Expire(ctx, zsetKey, 7*24*time.Hour)
	pipe.Expire(ctx, hashKey, 7*24*time.Hour)

	_, err = pipe.Exec(ctx)
	return err
}

// StartArchiveJob 定时将旧消息从 Redis 归档到 MySQL
func (s *StorageService) StartArchiveJob(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		s.archiveOldMessages()
	}
}

// archiveOldMessages 执行归档逻辑
func (s *StorageService) archiveOldMessages() {
	cutoff := time.Now().Add(-7 * 24 * time.Hour) // 7天前

	// 使用 SCAN 避免阻塞（生产推荐）
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = global.RedisDB.Scan(context.Background(), cursor, "conv:msg:*", 100).Result()
		if err != nil {
			return
		}

		for _, key := range keys {
			convID := key[len("conv:msg:"):]
			s.archiveConversation(convID, cutoff)
		}

		if cursor == 0 {
			break
		}
	}
}

// archiveConversation 归档单个会话的消息
func (s *StorageService) archiveConversation(convID string, cutoff time.Time) {
	key := fmt.Sprintf("conv:msg:%s", convID)

	// 查找过期消息
	msgs, err := global.RedisDB.ZRangeByScoreWithScores(context.Background(), key, &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%.0f", float64(cutoff.UnixNano())),
	}).Result()
	if err != nil || len(msgs) == 0 {
		return
	}

	var toArchive []*Message
	for _, z := range msgs {
		msgID := z.Member.(string)
		data, err := global.RedisDB.HGet(context.Background(), fmt.Sprintf("msg:%s", msgID), "data").Result()
		if err != nil {
			continue
		}
		msg := &Message{}
		if err := json.Unmarshal([]byte(data), msg); err != nil {
			continue
		}
		toArchive = append(toArchive, msg)
	}

	if len(toArchive) == 0 {
		return
	}

	// 批量写入 MySQL
	if err := global.DB.CreateInBatches(toArchive, 100).Error; err != nil {
		// 可记录日志，后续重试
		return
	}

	// 从 Redis 删除
	var members []interface{}
	for _, m := range toArchive {
		members = append(members, m.ID)
	}
	global.RedisDB.ZRem(context.Background(), key, members...)
	for _, m := range toArchive {
		global.RedisDB.Del(context.Background(), fmt.Sprintf("msg:%s", m.ID))
	}
}

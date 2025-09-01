package messagesave

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type RedisMessageStorage struct {
	client *redis.Client // 复用外部传入的 client
}

// 修改构造函数：接受已存在的 redis.Client
func NewRedisMessageStorage(client *redis.Client) *RedisMessageStorage {
	return &RedisMessageStorage{client: client}
}

// 后续 Save 和 List 方法保持不变（只使用 r.client）
func (r *RedisMessageStorage) Save(ctx context.Context, msg *Message) error {
	if msg.ID == "" || msg.ConversationID == "" || msg.Timestamp.IsZero() {
		return fmt.Errorf("消息缺少必要字段")
	}

	// 幂等控制
	if msg.ClientMsgID != "" {
		key := fmt.Sprintf("msgid:%s:%s", msg.ConversationID, msg.ClientMsgID)
		exists, err := r.client.Exists(ctx, key).Result()
		if err != nil {
			return err
		}
		if exists > 0 {
			return nil
		}
		r.client.SetEX(ctx, key, "1", 24*time.Hour)
	}

	// JSON 序列化
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("JSON 序列化失败: %v", err)
	}

	zsetKey := fmt.Sprintf("conv:msg:%s", msg.ConversationID)
	hashKey := fmt.Sprintf("msg:%s", msg.ID)
	score := float64(msg.Timestamp.UnixNano())

	pipe := r.client.TxPipeline()
	pipe.ZAdd(ctx, zsetKey, &redis.Z{Score: score, Member: msg.ID})
	pipe.HSet(ctx, hashKey, "data", string(data)) // 注意：HSet 第三参数是 interface{}，string 可以
	pipe.Expire(ctx, zsetKey, 7*24*time.Hour)
	pipe.Expire(ctx, hashKey, 7*24*time.Hour)

	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisMessageStorage) List(ctx context.Context, convID string, start, end time.Time, limit int, reverse bool) ([]*Message, error) {
	zsetKey := fmt.Sprintf("conv:msg:%s", convID)
	minScore := float64(start.UnixNano())
	maxScore := float64(end.UnixNano())

	zRange := &redis.ZRangeBy{
		Min:    fmt.Sprintf("%.0f", minScore),
		Max:    fmt.Sprintf("%.0f", maxScore),
		Offset: 0,
		Count:  int64(limit),
	}

	var ids []string
	var err error
	if reverse {
		ids, err = r.client.ZRevRangeByScore(ctx, zsetKey, zRange).Result()
	} else {
		ids, err = r.client.ZRangeByScore(ctx, zsetKey, zRange).Result()
	}

	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return []*Message{}, nil
	}

	pipe := r.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGet(ctx, fmt.Sprintf("msg:%s", id), "data")
	}
	if _, err = pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	var messages []*Message
	for _, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil {
			continue
		}
		msg := &Message{}
		if err := json.Unmarshal([]byte(data), msg); err != nil {
			continue
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

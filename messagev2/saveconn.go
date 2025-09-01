package messagev2

import (
	"HiChat/global"
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

const (
	// Redis Key 前缀
	UserConnPrefix = "user_conn:"
	// 连接有效期（TTL），心跳周期为 TTL 的 2/3
	ConnTTL = 30 * time.Second
)

// SetUserGateway 注册用户到指定网关节点
func SetUserGateway(userID, gatewayID string) error {
	key := UserConnPrefix + userID
	Ctx := context.Background()
	err := global.RedisDB.Set(Ctx, key, gatewayID, ConnTTL).Err()
	if err != nil {
		zap.S().Info("Redis SetUserGateway failed: user=%s, gateway=%s, err=%v", userID, gatewayID, err)
	} else {
		zap.S().Info("User %s registered to %s (TTL: %v)", userID, gatewayID, ConnTTL)
	}
	return err
}

// GetUserGateway 查询用户当前连接的网关节点
func GetUserGateway(userID string) (string, bool, error) {
	key := UserConnPrefix + userID
	Ctx := context.Background()
	gatewayID, err := global.RedisDB.Get(Ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", false, nil // 用户不在线
		}
		zap.S().Info("Redis GetUserGateway error: %v", err)
		return "", false, err
	}
	return gatewayID, true, nil
}

// RemoveUserGateway 用户断开时主动清除（可选）
func RemoveUserGateway(userID string) error {
	key := UserConnPrefix + userID
	Ctx := context.Background()
	err := global.RedisDB.Del(Ctx, key).Err()
	if err != nil {
		zap.S().Info("Redis RemoveUserGateway failed: %v", err)
	} else {
		zap.S().Info("User %s removed from Redis", userID)
	}
	return err
}

// IsUserOnline 判断用户是否在线
func IsUserOnline(userID string) (bool, error) {
	key := UserConnPrefix + userID
	Ctx := context.Background()
	exists, err := global.RedisDB.Exists(Ctx, key).Result()
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

func StartHeartbeat(userID string, stop <-chan struct{}) {
	ticker := time.NewTicker(ConnTTL / 2) // 每 15s 续约一次
	Ctx := context.Background()
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 刷新 TTL
			err := global.RedisDB.Expire(Ctx, UserConnPrefix+userID, ConnTTL).Err()
			if err != nil {
				zap.S().Info("Heartbeat failed for user %s: %v", userID, err)
				// 可尝试重连或退出
			} else {
				// log.Printf("Heartbeat renewed for user %s", userID)
			}
		case <-stop:
			// 连接断开，停止心跳
			zap.S().Info("Heartbeat stopped for user %s", userID)
			if err := RemoveUserGateway(userID); err != nil {
				zap.S().Info("Failed to remove user %s from Redis: %v", userID, err)
			} else {
				zap.S().Info("User %s successfully removed from Redis", userID)
			}
			return
		}
	}
}

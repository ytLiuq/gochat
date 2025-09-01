package messagesave

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// MySQLMessageStorage 使用外部传入的 *gorm.DB，不再自己创建
type MySQLMessageStorage struct {
	db *gorm.DB
}

// 构造函数改为接收已存在的 *gorm.DB
func NewMySQLMessageStorage(db *gorm.DB) *MySQLMessageStorage {
	// 确保表结构存在（可放在 migrations 中更好）
	db.AutoMigrate(&Message{})

	return &MySQLMessageStorage{db: db}
}

// Save 保存单条消息
func (m *MySQLMessageStorage) Save(ctx context.Context, msg *Message) error {
	return m.db.WithContext(ctx).Create(msg).Error
}

// List 按会话和时间范围查询消息（倒序：最新在前）
func (m *MySQLMessageStorage) List(ctx context.Context, convID string, start, end time.Time, limit int) ([]*Message, error) {
	var msgs []*Message
	err := m.db.WithContext(ctx).
		Where("conversation_id = ? AND timestamp > ? AND timestamp <= ?", convID, start, end).
		Order("timestamp DESC").
		Limit(limit).
		Find(&msgs).Error
	return msgs, err
}

// BatchSave 批量保存（归档时性能关键）
func (m *MySQLMessageStorage) BatchSave(ctx context.Context, messages []*Message) error {
	if len(messages) == 0 {
		return nil
	}
	return m.db.WithContext(ctx).CreateInBatches(messages, 100).Error
}

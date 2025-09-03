package messagesave

import (
	"context"
	"time"
)

type Message struct {
	ID             string    `json:"id" gorm:"column:id;primaryKey"`
	ConversationID string    `json:"conversation_id" gorm:"column:conversation_id;index:idx_conv_time"`
	SenderID       string    `json:"sender_id" gorm:"column:sender_id"`
	Content        []byte    `json:"content" gorm:"column:content"`
	MsgType        string    `json:"msg_type" gorm:"column:msg_type"`
	Timestamp      time.Time `json:"timestamp" gorm:"column:timestamp"`
	ClientMsgID    string    `json:"client_msg_id,omitempty" gorm:"-"` // 不存入数据库
}

// TableName GORM 表名
func (Message) TableName() string {
	return "messages_cold"
}

type MessageStorage interface {
	Save(ctx context.Context, msg *Message) error
	List(ctx context.Context, convID string, start, end time.Time, limit int) ([]*Message, error)
}

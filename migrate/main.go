package main

import (
	"HiChat/models"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	// ⚠️ 必须和 config.yaml 中的配置完全一致！
	dsn := "root:BAILAN201@tcp(127.0.0.1:3306)/hi_chat?charset=utf8mb4&parseTime=True&loc=Local"

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database: " + err.Error())
	}

	// 自动迁移表结构（不会删除已有数据）
	err = db.AutoMigrate(
		&models.UserBasic{},
		&models.Relation{},
		&models.Message{},
		&models.GroupInfo{},
		&models.Community{},
	)
	if err != nil {
		panic("failed to migrate database: " + err.Error())
	}

	println("✅ Database tables migrated successfully!")
}

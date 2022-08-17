package game

import (
	"bfmq/lib/db"
	"sync"
)

const (
	ONLINE uint8 = iota
	INROOM
	OFFLINE
)

type PlayerGame struct {
	PlayerLokc  sync.Mutex
	PlayerDB    *db.PlayerDB
	Status      uint8
	IdleNum     uint16
	PlayerDBNum uint16
	Roomid      uint32
}

func NewPlayerGame() *PlayerGame {
	return &PlayerGame{
		PlayerDB: &db.PlayerDB{},
		Status:   ONLINE,
	}
}

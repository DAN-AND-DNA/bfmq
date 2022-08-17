package lockstep

import (
	"bfmq/msg"
	"errors"
	"sync"
)

type RoomMgr struct {
	Rooms        map[uint32]*Room
	CanJoinRooms []*Room
	ManagerLock  sync.Mutex
	RoomGen      uint32
}

var RoomMgr_g *RoomMgr

func init() {
	RoomMgr_g = &RoomMgr{
		Rooms:        make(map[uint32]*Room),
		CanJoinRooms: make([]*Room, 0, 10),
		RoomGen:      1000,
	}
}

func (this *RoomMgr) Count() int {
	return len(this.Rooms)
}

func (this *RoomMgr) JoinRandomRoomm(a *msg.Agent, maxLimit uint32) (uint32, uint32, uint32, error) {
	this.ManagerLock.Lock()
	defer this.ManagerLock.Unlock()
	var room *Room
	if len(this.CanJoinRooms) > 0 {
		index := len(this.CanJoinRooms) - 1
		room = this.CanJoinRooms[index]
		this.CanJoinRooms = this.CanJoinRooms[:index]
	} else {
		this.RoomGen += 1
		roomid := this.RoomGen
		room = NewRoom(roomid, maxLimit)
		this.Rooms[roomid] = room
	}

	if playerid, seed, err := room.JoinRoom(a); err == nil {
		if !room.IsFull() {
			this.CanJoinRooms = append(this.CanJoinRooms, room)
		}
		return room.Roomid, playerid, seed, nil
	} else {
		return 0, 0, 0, err
	}
}

func (this *RoomMgr) JoinRandomRoom(a *msg.Agent, maxLimit uint32) (uint32, uint32, uint32, error) {
	var room *Room
	if len(this.CanJoinRooms) > 0 {
		index := len(this.CanJoinRooms) - 1
		room = this.CanJoinRooms[index]
		this.CanJoinRooms = this.CanJoinRooms[:index]
	} else {
		this.RoomGen += 1
		roomid := this.RoomGen
		room = NewRoom(roomid, maxLimit)
		this.Rooms[roomid] = room
	}

	if playerid, seed, err := room.JoinRoom(a); err == nil {
		if !room.IsFull() {
			this.CanJoinRooms = append(this.CanJoinRooms, room)
		}
		return room.Roomid, playerid, seed, nil
	} else {
		return 0, 0, 0, err
	}
}

// call at main go
func (this *RoomMgr) FakeJoinRoom(roomid uint32) error {
	room := this.Rooms[roomid]

	if room != nil {
		if room.IsFull() {
			return errors.New("room is full")
		} else {
			if room.GetPlayerCount() > 0 {
				if err := room.FillFake(); err == nil {
					return nil
				} else {
					return err
				}
			} else {
				return errors.New("room no man")
			}
		}
	} else {
		return errors.New("no such room")
	}
}

func (this *RoomMgr) GetRoomm(roomid uint32) (*Room, bool) {
	this.ManagerLock.Lock()
	defer this.ManagerLock.Unlock()

	if room, ok := this.Rooms[roomid]; ok == true {
		return room, true
	} else {
		return nil, false
	}
}

func (this *RoomMgr) GetRoom(roomid uint32) (*Room, bool) {
	if room, ok := this.Rooms[roomid]; ok == true {
		return room, true
	} else {
		return nil, false
	}
}

func (this *RoomMgr) RemoveRoomm(roomid uint32) {
	this.ManagerLock.Lock()
	defer this.ManagerLock.Unlock()
	delete(this.Rooms, roomid)
}

func (this *RoomMgr) RemoveRoom(roomid uint32) {
	delete(this.Rooms, roomid)
}

func (this *RoomMgr) RemoveCanJoinRoomm(roomid uint32) {
	this.ManagerLock.Lock()
	defer this.ManagerLock.Unlock()

	canJoinRoomsLen := len(this.CanJoinRooms)

	for i := 0; i < canJoinRoomsLen; i++ {
		if this.CanJoinRooms[i].Roomid == roomid {
			if i == canJoinRoomsLen-1 {
				// the last one
				this.CanJoinRooms = this.CanJoinRooms[:i]
			} else {
				// the middle one
				this.CanJoinRooms = append(this.CanJoinRooms[:i], this.CanJoinRooms[i+1:]...)
			}
		}
	}
}

func (this *RoomMgr) RemoveCanJoinRoom(roomid uint32) {
	canJoinRoomsLen := len(this.CanJoinRooms)

	for i := 0; i < canJoinRoomsLen; i++ {
		if this.CanJoinRooms[i].Roomid == roomid {
			if i == canJoinRoomsLen-1 {
				// the last one
				this.CanJoinRooms = this.CanJoinRooms[:i]
			} else {
				// the middle one
				this.CanJoinRooms = append(this.CanJoinRooms[:i], this.CanJoinRooms[i+1:]...)
			}
		}
	}
}

func (this *RoomMgr) LeaveRoomm(roomId uint32, userid uint64) {
	this.ManagerLock.Lock()
	defer this.ManagerLock.Unlock()

	if room, ok := this.Rooms[roomId]; ok == true {
		roomstatus := room.GetStatus()
		if roomstatus == WAITTING {
			room.LeaveRoom(userid)
			if room.GetPlayerCount() == room.GetOfflineCount() {
				room.StopLoopPush()
				delete(this.Rooms, roomId)
				canJoinRoomsLen := len(this.CanJoinRooms)

				for i := 0; i < canJoinRoomsLen; i++ {
					if this.CanJoinRooms[i].Roomid == roomId {
						if i == canJoinRoomsLen-1 {
							this.CanJoinRooms = this.CanJoinRooms[:i]
						} else {
							this.CanJoinRooms = append(this.CanJoinRooms[:i], this.CanJoinRooms[i+1:]...)
						}
					}
				}
			}
		} else if roomstatus == BEFORERUNNING {
			room.DoLostConnection(userid)
		} else {
			room.DoLostConnection(userid)
		}
	}
}

func (this *RoomMgr) LeaveRoom(roomId uint32, userid uint64) {
	if room, ok := this.Rooms[roomId]; ok == true {
		roomstatus := room.GetStatus()
		if roomstatus == WAITTING {
			room.LeaveRoom(userid)
			if room.GetPlayerCount() == room.GetOfflineCount() {
				room.StopLoopPush()
				delete(this.Rooms, roomId)
				canJoinRoomsLen := len(this.CanJoinRooms)

				for i := 0; i < canJoinRoomsLen; i++ {
					if this.CanJoinRooms[i].Roomid == roomId {
						if i == canJoinRoomsLen-1 {
							this.CanJoinRooms = this.CanJoinRooms[:i]
						} else {
							this.CanJoinRooms = append(this.CanJoinRooms[:i], this.CanJoinRooms[i+1:]...)
						}
					}
				}
			}
		} else if roomstatus == BEFORERUNNING {
			room.DoLostConnection(userid)
		} else {
			room.DoLostConnection(userid)
		}
	}
}

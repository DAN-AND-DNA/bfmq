package tinypool

import (
	"bfmq/lib/lockstep"
	"bfmq/lib/tinywheel"
	"bfmq/msg"
	"log"
)

var IdleMgr_g *tinywheel.TinyWheel
var Time2PVEMgr_g *tinywheel.TinyWheel
var Time2PlayMgr_g *tinywheel.TinyWheel

func PreRun(idletime uint16, time2pve uint16, time2play uint16) {

	//1. for idle
	temp := tinywheel.Entry{
		Conns: make(map[interface{}]interface{}),
		Num:   1,
		Next:  nil,
	}

	temp.Next = &temp
	IdleMgr_g = &tinywheel.TinyWheel{
		MaxTime: 0,
		Tail:    nil,
		Head:    &temp,
		Chucks:  make([]*tinywheel.Entry, idletime, idletime+1),
	}

	IdleMgr_g.MaxTime = idletime
	IdleMgr_g.Chucks[1] = &temp

	i := (uint16)(2)
	for ; i < IdleMgr_g.MaxTime; i++ {
		temp := &tinywheel.Entry{
			Conns: make(map[interface{}]interface{}),
			Num:   i,
			Next:  IdleMgr_g.Head.Next,
		}
		IdleMgr_g.Head.Next = temp
		IdleMgr_g.Tail = IdleMgr_g.Head
		IdleMgr_g.Chucks[i] = temp
	}

	//2. for pve
	temp1 := tinywheel.Entry{
		Conns: make(map[interface{}]interface{}),
		Num:   1,
		Next:  nil,
	}

	temp1.Next = &temp1
	Time2PVEMgr_g = &tinywheel.TinyWheel{
		MaxTime: 0,
		Tail:    nil,
		Head:    &temp1,
		Chucks:  make([]*tinywheel.Entry, time2pve, time2pve+1),
	}

	Time2PVEMgr_g.MaxTime = time2pve
	Time2PVEMgr_g.Chucks[1] = &temp1

	j := (uint16)(2)
	for ; j < Time2PVEMgr_g.MaxTime; j++ {
		temp1 := &tinywheel.Entry{
			Conns: make(map[interface{}]interface{}),
			Num:   j,
			Next:  Time2PVEMgr_g.Head.Next,
		}
		Time2PVEMgr_g.Head.Next = temp1
		Time2PVEMgr_g.Tail = Time2PVEMgr_g.Head
		Time2PVEMgr_g.Chucks[j] = temp1
	}

	//3. for time to play
	temp2 := tinywheel.Entry{
		Conns: make(map[interface{}]interface{}),
		Num:   1,
		Next:  nil,
	}

	temp2.Next = &temp2
	Time2PlayMgr_g = &tinywheel.TinyWheel{
		MaxTime: 0,
		Tail:    nil,
		Head:    &temp2,
		Chucks:  make([]*tinywheel.Entry, time2play, time2play+1),
	}

	Time2PlayMgr_g.MaxTime = time2play
	Time2PlayMgr_g.Chucks[1] = &temp2

	h := (uint16)(2)
	for ; h < Time2PlayMgr_g.MaxTime; h++ {
		temp2 := &tinywheel.Entry{
			Conns: make(map[interface{}]interface{}),
			Num:   h,
			Next:  Time2PlayMgr_g.Head.Next,
		}
		Time2PlayMgr_g.Head.Next = temp2
		Time2PlayMgr_g.Tail = Time2PlayMgr_g.Head
		Time2PlayMgr_g.Chucks[h] = temp2
	}

}

func KickIdles() {
	idles := *IdleMgr_g.Loop()
	if len(idles) > 0 {
		log.Println("kick idle")
	}
	for k, _ := range idles {
		if a, ok := k.(*msg.Agent); ok == true {
			a.Close()
		}
	}
}

func Time2PVE() {
	rooms := *Time2PVEMgr_g.Loop()
	if len(rooms) > 0 {
		log.Println("time to pve")
	}
	for k, _ := range rooms {
		if roomid, ok := k.(uint32); ok == true {
			if room, ok := lockstep.RoomMgr_g.GetRoom(roomid); ok {
				if err := lockstep.RoomMgr_g.FakeJoinRoom(roomid); err == nil {
					room.SetStatus(lockstep.BEFORERUNNING)
					lockstep.RoomMgr_g.RemoveCanJoinRoom(roomid)
					room.Notify(0, "CMD_GAME_ENTERGAME_NOTIFY_BROADCAST")
					room.Time2PlayNum = Time2PlayMgr_g.Add(roomid, 7)
				} else {
					log.Printf("fake join error:%v", err)
				}
			}
		}

	}
}

func Time2Play() {
	rooms := *Time2PlayMgr_g.Loop()
	if len(rooms) > 0 {
		log.Println("time to play")
	}
	for k, _ := range rooms {
		if roomid, ok := k.(uint32); ok == true {
			if room, ok := lockstep.RoomMgr_g.GetRoom(roomid); ok {
				log.Println("kick idle")
				room.KickIdle()
				log.Println("start loop")
				room.SetStatus(lockstep.RUNNING)
				room.StartLoopPush()
			}
		}

	}
}

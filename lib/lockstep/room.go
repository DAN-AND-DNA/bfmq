package lockstep

import (
	"bfmq/lib/game"
	"bfmq/msg"
	"errors"
	"github.com/golang/protobuf/proto"
	"log"
	"math/rand"
	"time"
)

var RoomInQueue chan *Task
var RoomOutQueue chan *Task

const (
	IN uint8 = iota + 1
	OUT
	LEAVE
)

type Task struct {
	Cmd uint8
	Msg *msg.C2S
	R   *Room
	U   uint64
}

func init() {
	RoomInQueue = make(chan *Task)
	RoomOutQueue = make(chan *Task)
}

const (
	WAITTING uint8 = iota
	BEFORERUNNING
	RUNNING
	GAMEOVER
)

type Room struct {
	Roomid          uint32
	Players         map[uint64]*PlayerRoom
	MaxLimit        uint32
	Status          uint8
	RoomPlayerIdGen uint32
	Time2PVENum     uint16
	Time2PlayNum    uint16
	IsStartLoopPush bool
	StepQueue       *QuickSlice
	LeaveQueue      *QuickSlice1
	LeaveChan       chan uint64
	MsgChan         chan *msg.C2S
	TimerChan       chan struct{}
	DoneChan        chan struct{}
	StepNum         int32
	MSendTime       time.Time
	Seed            uint32
	FrameLen        int64 //ns
}

func NewRoom(roomid uint32, maxLimit uint32) *Room {

	frameSpeed := (float64)(20) // 20 frames per second

	r := &Room{
		Roomid:          roomid,
		Players:         make(map[uint64]*PlayerRoom),
		MaxLimit:        maxLimit,
		Status:          WAITTING,
		RoomPlayerIdGen: 0,
		IsStartLoopPush: false,
		StepQueue:       NewQuickSlice(0, 4),
		LeaveQueue:      NewQuickSlice1(0, 4),
		LeaveChan:       make(chan uint64, 7),
		MsgChan:         make(chan *msg.C2S, 100000),
		TimerChan:       make(chan struct{}, 10000),
		DoneChan:        make(chan struct{}, 3),
		StepNum:         -1,
		MSendTime:       time.Now(),
		Seed:            0,
		FrameLen:        int64(1.0 / frameSpeed * 1000000000),
	}

	rand.Seed(r.MSendTime.UnixNano())
	r.Seed = (uint32)(rand.Intn(99) + 1)

	go func() {
		for {
			select {
			case <-r.TimerChan:
				if r.IsStartLoopPush && (r.GetPlayerCount() > r.GetOfflineCount()) {
					// somebody is playing
					r.Step()
				} else {
					log.Printf("roomid:%d gameover at step:%d", r.Roomid, r.StepNum)
					// gameover!
					r.DoneChan <- struct{}{}
					RoomOutQueue <- &Task{Cmd: OUT, R: r}
					return
				}

			case m := <-r.MsgChan:
				r.AddUserInput(m.GetInputReq().GetInput())
			case userid := <-r.LeaveChan:
				r.LeaveRoom(userid)
			}
		}
	}()

	return r
}

func (this *Room) GetStatus() uint8 {
	return this.Status
}

func (this *Room) SetStatus(Status uint8) {
	this.Status = Status
}

func (this *Room) SendMsg(userid uint64, msg *msg.C2S) {
	if p, ok := this.GetPlayer(userid); ok {
		p.SendMsg(msg)
	}
}

func (this *Room) GetPlayer(userid uint64) (*PlayerRoom, bool) {
	if p, ok := this.Players[userid]; ok == true {
		return p, ok
	} else {
		return nil, false
	}
}

func (this *Room) GetPlayerCount() uint32 {
	return (uint32)(len(this.Players))
}

func (this *Room) IsFull() bool {
	if this.GetPlayerCount() == this.MaxLimit {
		return true
	}
	return false
}

func (this *Room) IsAllInStateN(n PLAYERSTATE) bool {
	for _, p := range this.Players {
		if p.GetStatus() != n {
			return false
		}
	}
	return true
}

func (this *Room) KickIdle() {
	for userid, p := range this.Players {
		if p.GetStatus() != ALREADY {
			delete(this.Players, userid)
		}
	}
}

func (this *Room) IsAllReady() bool {
	if this.IsFull() && this.IsAllInStateN(ALREADY) {
		return true
	}
	return false
}

func (this *Room) GetOfflineCount() uint32 {
	var count uint32 = 0
	for _, p := range this.Players {
		if !p.IsOnline() {
			count++
		}
	}
	return count
}

func (this *Room) DoLostConnection(userid uint64) bool {
	if p, ok := this.GetPlayer(userid); ok {
		p.LostConnection()

		if this.GetPlayerCount() == this.GetOfflineCount() {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func (this *Room) Notify(userid uint64, Cmd string) int {

	switch Cmd {
	case "CMD_GAME_PLAYERJOIN_NOTIFY_ALL":
		cmd := msg.C2Scmd_CMD_GAME_PLAYERJOIN_NOTIFY
		t := msg.C2Stype_TYPE_MAIN
		resmsg := &msg.C2S{
			Head: &msg.C2SHead{Type: &t, Seq: (proto.Uint32)(0),
				Cmd: &cmd},
			Result:           &msg.C2SRes{},
			PlayerJoinNotify: &msg.PlayerJoinNotify{},
		}

		for _, temp := range this.Players {
			pUserid, _ := temp.GetUserid()
			if pUserid > 1000 {
				playertype := msg.Playertype_MAN
				resmsg.PlayerJoinNotify.Type = &playertype
			} else {
				playertype := msg.Playertype_FAKE
				resmsg.PlayerJoinNotify.Type = &playertype

			}
			resmsg.PlayerJoinNotify.Nickname = (proto.String)(temp.Nickname)
			resmsg.PlayerJoinNotify.AvatarUrl = (proto.String)(temp.AvatarUrl)
			resmsg.PlayerJoinNotify.Playerid = (proto.Uint32)(temp.Playerid)
			log.Printf("send cmd: %v", resmsg.GetHead().GetCmd())
			this.SendMsg(userid, resmsg)
		}
	case "CMD_GAME_PLAYERJOIN_NOTIFY_MULTICAST":
		cmd := msg.C2Scmd_CMD_GAME_PLAYERJOIN_NOTIFY
		t := msg.C2Stype_TYPE_MAIN
		resmsg := &msg.C2S{
			Head: &msg.C2SHead{Type: &t, Seq: (proto.Uint32)(0),
				Cmd: &cmd},
			Result:           &msg.C2SRes{},
			PlayerJoinNotify: &msg.PlayerJoinNotify{},
		}
		p := this.Players[userid]

		for pUserid, _ := range this.Players {

			if pUserid != userid && pUserid > 1000 {
				resmsg.PlayerJoinNotify.Playerid = (proto.Uint32)(p.Playerid)

				if userid < 1000 {
					playertype := msg.Playertype_FAKE
					resmsg.PlayerJoinNotify.Type = &playertype
				} else {
					playertype := msg.Playertype_MAN
					resmsg.PlayerJoinNotify.Type = &playertype
				}
				resmsg.PlayerJoinNotify.Nickname = (proto.String)(p.Nickname)
				resmsg.PlayerJoinNotify.AvatarUrl = (proto.String)(p.AvatarUrl)

				log.Printf("send cmd: %v", resmsg.GetHead().GetCmd())
				this.SendMsg(pUserid, resmsg)
			}
		}

	case "CMD_GAME_PLAYERLEAVE_NOTIFY_BROADCAST":
		cmd := msg.C2Scmd_CMD_GAME_PLAYERLEAVE_NOTIFY
		t := msg.C2Stype_TYPE_MAIN
		resmsg := &msg.C2S{
			Head: &msg.C2SHead{Type: &t, Seq: (proto.Uint32)(0),
				Cmd: &cmd},
			Result:            &msg.C2SRes{},
			PlayerLeaveNotify: &msg.PlayerLeaveNotify{},
		}

		p := this.Players[userid]
		for pUserid, temp := range this.Players {
			//pUserid, _ := temp.GetUserid()
			if !temp.IsOnline() {
				return 0
			}

			resmsg.PlayerLeaveNotify.Playerid = (proto.Uint32)(p.Playerid)

			log.Printf("send cmd: %v", resmsg.GetHead().GetCmd())
			if temp.IsOnline() {
				this.SendMsg(pUserid, resmsg)
			}
		}

	case "CMD_GAME_INPUT_NOTIFY_BROADCAST":
		cmd := msg.C2Scmd_CMD_GAME_INPUT_NOTIFY
		t := msg.C2Stype_TYPE_MAIN
		resmsg := &msg.C2S{
			Head: &msg.C2SHead{Type: &t, Seq: (proto.Uint32)(0),
				Cmd: &cmd},
			Result:      &msg.C2SRes{},
			InputNotify: &msg.InputNotify{},
		}
		var inputlist []*msg.InputData = this.StepQueue.GetAll()

		resmsg.InputNotify.Step = (proto.Int32)(this.StepNum)
		left := len(inputlist)
		if left != 0 {
			resmsg.InputNotify.InputList = inputlist
		}

		for _, temp := range this.Players {
			pUserid, _ := temp.GetUserid()

			if pUserid > 1000 {
				this.SendMsg(pUserid, resmsg)
			}
		}
		if left == 0 {
			return 1
		}
	case "CMD_GAME_ENTERGAME_NOTIFY_BROADCAST":
		cmd := msg.C2Scmd_CMD_GAME_ENTERGAME_NOTIFY
		t := msg.C2Stype_TYPE_MAIN
		resmsg := &msg.C2S{
			Head: &msg.C2SHead{Type: &t, Seq: (proto.Uint32)(0),
				Cmd: &cmd},
			Result:          &msg.C2SRes{},
			EnterGameNotify: &msg.EnterGameNotify{},
		}

		resmsg.EnterGameNotify.Roomid = (proto.Uint32)(this.Roomid)

		for pUserid, _ := range this.Players {
			if pUserid > 1000 {
				log.Printf("send cmd: %v", resmsg.GetHead().GetCmd())
				this.SendMsg(pUserid, resmsg)
			}
		}
	case "CMD_GAME_STARTGAME_NOTIFY":
		cmd := msg.C2Scmd_CMD_GAME_STARTGAME_NOTIFY
		t := msg.C2Stype_TYPE_MAIN
		resmsg := &msg.C2S{
			Head: &msg.C2SHead{Type: &t, Seq: (proto.Uint32)(0),
				Cmd: &cmd},
			Result:          &msg.C2SRes{},
			StartGameNotify: &msg.StartGameNotify{},
		}

		resmsg.StartGameNotify.Time = (proto.Uint64)((uint64)(this.MSendTime.UnixNano() / 1000000))

		for pUserid, _ := range this.Players {
			if pUserid > 1000 {
				log.Printf("send cmd: %v", resmsg.GetHead().GetCmd())
				this.SendMsg(pUserid, resmsg)
			}
		}

	default:
		return 0
	}
	return 0
}

func (this *Room) JoinRoom(a *msg.Agent) (uint32, uint32, error) {
	p1 := a.UserData
	playergame, ok := p1.(*game.PlayerGame)

	if ok == true {
	} else {
		return 0, 0, errors.New("no player game")
	}

	userid := playergame.PlayerDB.GetUserid()

	if userid == 0 {
		return 0, 0, errors.New("no userid")
	}

	p, ok := this.GetPlayer(userid)
	if ok {
		// reconnect when waiting for other players
		p.ReConnection(a)
		return p.Playerid, this.Seed, nil
	} else {
		this.RoomPlayerIdGen++
		p2 := NewPlayerRoom(a, this.RoomPlayerIdGen, this.Seed)
		this.Players[userid] = p2
		return p2.Playerid, this.Seed, nil

	}
}

func (this *Room) FillFake() error {

	needfake := (int)(this.MaxLimit) - len(this.Players)

	log.Printf("add fake player:%d", needfake)
	// userid of fake player < 1000

	for i := 1; i < needfake+1; i++ {
		ulUserid := (uint64)(i)
		this.RoomPlayerIdGen++
		p := NewPlayerRoom(nil, this.RoomPlayerIdGen, this.Seed)

		this.Players[ulUserid] = p
		this.Notify(ulUserid, "CMD_GAME_PLAYERJOIN_NOTIFY_MULTICAST") // tell other players
	}

	return nil
}

func (this *Room) LeaveRoom(userid uint64) {
	this.Notify(userid, "CMD_GAME_PLAYERLEAVE_NOTIFY_BROADCAST")
	delete(this.Players, userid)
}

func (this *Room) SendStartGame() {
	this.Notify(0, "CMD_GAME_STARTGAME_NOTIFY")
}

func (this *Room) StartLoopPush() {
	if !this.IsStartLoopPush {
		//this.MSendTime = time.Now()
		this.SendStartGame()
		this.IsStartLoopPush = true
		this.LoopPush()
	}
}

func (this *Room) Step() {
	/*
		var userids []uint64 = this.LeaveQueue.GetAll()
		for _, userid := range userids {
			this.LeaveRoom(userid)
		}
	*/

	this.StepNum++

	_ = this.Notify(0, "CMD_GAME_INPUT_NOTIFY_BROADCAST")

	if this.StepNum >= 3610 {
		this.StopLoopPush()
		return
	}
}

func (this *Room) AddUserInput(userData *msg.InputData) {
	this.StepQueue.Append(userData)
}

func (this *Room) AddUserInputm(userData *msg.InputData) {
	this.StepQueue.Appendm(userData)
}

func (this *Room) AddLeave(userid uint64) {
	this.LeaveQueue.Append(userid)
}

func (this *Room) StopLoopPush() {
	if this.IsStartLoopPush {
		this.IsStartLoopPush = false
	}
}

func (this *Room) LoopPush() {
	/*
		go func() {
			for {
				if this.IsStartLoopPush && (this.GetPlayerCount() > this.GetOfflineCount()) {
					// somebody is playing
					start := time.Now()
					this.Step()
					needSleepTime := this.FrameLen - (time.Now().Sub(start).Nanoseconds())
					time.Sleep(time.Duration(needSleepTime))
					//log.Debug("sleep time: %v", (time.Now().Sub(start).Nanoseconds() / 1000000))
				} else {
					log.Printf("roomid:%d gameover at step:%d", this.Roomid, this.StepNum)
					// gameover!
					RoomOutQueue <- &Task{Cmd: OUT, R: this}
					return
				}
			}

		}()
	*/

	go func() {
		for {
			select {
			case <-this.DoneChan:
				return
			default:
				this.TimerChan <- struct{}{}
				time.Sleep(time.Duration(this.FrameLen))
			}
		}
	}()

}

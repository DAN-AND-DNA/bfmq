package mod

import (
	"bfmq/lib/game"
	"bfmq/lib/lockstep"
	"bfmq/lib/tinypool"
	"bfmq/msg"
	"github.com/golang/protobuf/proto"
	"log"
)

var ub uint64

func init() {
	ub = 10000
}

func HandleMsg(task *msg.Task) {
	a := task.A
	m := task.Msg
	switch task.Cmd {
	case msg.MSG:
		subcmd := m.GetHead().GetCmd()
		if subcmd != msg.C2Scmd_CMD_GAME_INPUT_REQ {
			log.Printf("get cmd: %v", subcmd)
		}
		switch m.GetHead().GetCmd() {
		case msg.C2Scmd_CMD_LOGIN_GET_OPENID_REQ:
			cmd := msg.C2Scmd_CMD_LOGIN_GET_OPENID_RES
			seq := m.GetHead().GetSeq()
			t := m.GetHead().GetType()
			resmsg := &msg.C2S{
				Head:         &msg.C2SHead{Type: &t, Seq: &seq, Cmd: &cmd},
				Result:       &msg.C2SRes{},
				GetOpenidRes: &msg.GetOpenidRes{},
			}
			code := m.GetGetOpenidReq().GetCode()
			if code == "wxtestcode" {
				resmsg.Result.Res = (proto.Int32)(0)
				resmsg.GetOpenidRes.Openid = (proto.String)("oeZPm5eg7cGQkUXTh_t3aS4NxD37")
			} else {
				resmsg.Result.Res = (proto.Int32)(-1)
			}
			a.Send(resmsg)

		case msg.C2Scmd_CMD_LOGIN_GET_DATA_REQ:
			p := a.UserData
			playergame, ok := p.(*game.PlayerGame)

			if ok == true {
			} else {
				a.Close()
				return
			}

			cmd := msg.C2Scmd_CMD_LOGIN_GET_DATA_RES
			seq := m.GetHead().GetSeq()
			t := m.GetHead().GetType()
			resmsg := &msg.C2S{
				Head:       &msg.C2SHead{Type: &t, Seq: &seq, Cmd: &cmd},
				Result:     &msg.C2SRes{},
				GetDataRes: &msg.GetDataRes{},
			}

			openid := m.GetGetDataReq().GetOpenid()
			if openid == "oeZPm5eg7cGQkUXTh_t3aS4NxD37" {
				resmsg.Result.Res = (proto.Int32)(0)
				//playergame.PlayerDB.Userid = (proto.Uint64)(10001)
				ub++
				playergame.PlayerDB.Userid = (proto.Uint64)(ub)
				playergame.PlayerDB.Openid = (proto.String)(openid)
				playergame.PlayerDB.Nickname = (proto.String)("游客")
				playergame.PlayerDB.AvatarUrl = (proto.String)("https://wx.qlogo.cn/mmopen/vi_32/Q0j4TwGTfTJ5q5ml6p7iabwUtLYgiajsOFYwErJRposhexJu734UUcHK6CJJ1J3YpKUkW9QNDdyr3uLl9A5q6f2g/132")
				playergame.PlayerDB.Gold = (proto.Uint32)(1000)

			} else {
				resmsg.Result.Res = (proto.Int32)(-1)
			}
			resmsg.GetDataRes.Player = &msg.C2SPlayer{
				Userid:    (proto.Uint64)(playergame.PlayerDB.GetUserid()),
				Nickname:  (proto.String)(playergame.PlayerDB.GetNickname()),
				AvatarUrl: (proto.String)(playergame.PlayerDB.GetAvatarUrl()),
				Gold:      (proto.Uint32)(playergame.PlayerDB.GetGold()),
			}
			a.Send(resmsg)

		case msg.C2Scmd_CMD_LOGIN_HEARTBEAT_REQ:
			p := a.UserData
			playergame, ok := p.(*game.PlayerGame)

			_ = playergame
			if ok == true {
			} else {
				a.Close()
				return
			}

			cmd := msg.C2Scmd_CMD_LOGIN_HEARTBEAT_RES
			seq := m.GetHead().GetSeq()
			t := m.GetHead().GetType()
			resmsg := &msg.C2S{
				Head:         &msg.C2SHead{Type: &t, Seq: &seq, Cmd: &cmd},
				Result:       &msg.C2SRes{},
				HeartBeatRes: &msg.HeartBeatRes{},
			}
			playergame.IdleNum = tinypool.IdleMgr_g.Keep(a, playergame.IdleNum, 2)
			a.Send(resmsg)

		case msg.C2Scmd_CMD_GAME_JOINROOM_REQ:
			p := a.UserData
			playergame, ok := p.(*game.PlayerGame)

			if ok == true {
			} else {
				a.Close()
				return
			}

			cmd := msg.C2Scmd_CMD_GAME_JOINROOM_RES
			seq := m.GetHead().GetSeq()
			userid := playergame.PlayerDB.GetUserid()
			t := m.GetHead().GetType()
			resmsg := &msg.C2S{
				Head:        &msg.C2SHead{Type: &t, Seq: &seq, Cmd: &cmd},
				Result:      &msg.C2SRes{},
				JoinRoomRes: &msg.JoinRoomRes{},
			}

			if playergame.Status != game.ONLINE {
				resmsg.Result.Res = (proto.Int32)(-1)
				a.Send(resmsg)
				return
			}

			if roomid, playerid, seed, err := lockstep.RoomMgr_g.JoinRandomRoom(a, 6); err == nil {
				playergame.Status = game.INROOM
				playergame.Roomid = roomid
				if room, ok := lockstep.RoomMgr_g.GetRoom(roomid); ok {
					if room.IsFull() {
						room.SetStatus(lockstep.BEFORERUNNING)
						lockstep.RoomMgr_g.RemoveCanJoinRoom(roomid)
						resmsg.JoinRoomRes.Roomid = (proto.Uint32)(roomid)
						resmsg.JoinRoomRes.Playerid = (proto.Uint32)(playerid)
						resmsg.JoinRoomRes.Seed = (proto.Uint32)(seed)
						a.Send(resmsg)
						room.Notify(userid, "CMD_GAME_PLAYERJOIN_NOTIFY_MULTICAST")
						room.Notify(userid, "CMD_GAME_PLAYERJOIN_NOTIFY_ALL")
						room.Notify(0, "CMD_GAME_ENTERGAME_NOTIFY_BROADCAST")
						room.Time2PlayNum = tinypool.Time2PlayMgr_g.Add(roomid, 7)
					} else {
						room.SetStatus(lockstep.WAITTING)
						resmsg.JoinRoomRes.Roomid = (proto.Uint32)(roomid)
						resmsg.JoinRoomRes.Playerid = (proto.Uint32)(playerid)
						resmsg.JoinRoomRes.Seed = (proto.Uint32)(seed)
						a.Send(resmsg)
						room.Notify(userid, "CMD_GAME_PLAYERJOIN_NOTIFY_MULTICAST")
						room.Notify(userid, "CMD_GAME_PLAYERJOIN_NOTIFY_ALL")
						if room.Time2PVENum != 0 {
							room.Time2PVENum = tinypool.Time2PVEMgr_g.Keep(roomid, room.Time2PVENum, 7)
						} else {
							room.Time2PVENum = tinypool.Time2PVEMgr_g.Add(roomid, 7)
						}

					}
				}
			} else {
				log.Printf("join room err:%v", err)
				resmsg.Result.Res = (proto.Int32)(-1)
				a.Send(resmsg)
			}

		case msg.C2Scmd_CMD_GAME_READY_REQ:
			p := a.UserData
			playergame, ok := p.(*game.PlayerGame)

			if ok == true {
			} else {
				a.Close()
				return
			}

			cmd := msg.C2Scmd_CMD_GAME_READY_RES
			seq := m.GetHead().GetSeq()
			t := m.GetHead().GetType()
			roomid := m.GetReadyReq().GetRoomid()
			userid := playergame.PlayerDB.GetUserid()

			resmsg := &msg.C2S{
				Head:     &msg.C2SHead{Type: &t, Seq: &seq, Cmd: &cmd},
				Result:   &msg.C2SRes{},
				ReadyRes: &msg.ReadyRes{},
			}

			if playergame.Status != game.INROOM {
				resmsg.Result.Res = (proto.Int32)(-1)
				a.Send(resmsg)
				return
			}

			if room, ok := lockstep.RoomMgr_g.GetRoom(roomid); ok {
				if p, ok := room.GetPlayer(userid); ok {
					p.SetStatus(lockstep.ALREADY)
				}

				resmsg.ReadyRes.Roomid = (proto.Uint32)(roomid)
				a.Send(resmsg)

				if room.IsAllReady() {
					if room.Time2PlayNum != 0 {
						tinypool.Time2PlayMgr_g.Del(roomid, room.Time2PlayNum)
					}
					room.SetStatus(lockstep.RUNNING)
					room.StartLoopPush()
					return
				}
			} else {
				resmsg.Result.Res = (proto.Int32)(-1)
				resmsg.ReadyRes.Roomid = (proto.Uint32)(roomid)
				a.Send(resmsg)
			}

		case msg.C2Scmd_CMD_GAME_INPUT_REQ:
			roomid := m.GetInputReq().GetRoomid()
			if room, ok := lockstep.RoomMgr_g.GetRoom(roomid); ok {
				room.MsgChan <- m
				//room.AddUserInputm(m.GetInputReq().GetInput())
			} else {
				log.Printf("no such roomid:%d", roomid)
			}
		default:
			a.Close()
		}
	case msg.NEW:
		log.Println("online")
		playergame := game.NewPlayerGame()
		a.UserData = playergame
		playergame.Status = game.ONLINE
		playergame.IdleNum = tinypool.IdleMgr_g.Add(a, 2)

	case msg.CLOSE:
		log.Println("offline")
		p := a.UserData
		playergame, ok := p.(*game.PlayerGame)
		if ok {
			tinypool.IdleMgr_g.Del(a, playergame.IdleNum)
		}
		userid := playergame.PlayerDB.GetUserid()
		roomid := playergame.Roomid
		status := playergame.Status
		if status == game.INROOM && roomid != 0 {
			if room, ok := lockstep.RoomMgr_g.GetRoom(roomid); ok {
				room.LeaveChan <- userid
			} else {
				log.Printf("no such roomid:%d", roomid)
			}
		}
		playergame.Status = game.OFFLINE
	default:
		a.Close()
	}
}

func HandleRoom(task *lockstep.Task) {
	room := task.R
	switch task.Cmd {
	case lockstep.IN:
		m := task.Msg
		if m.GetHead().GetCmd() == msg.C2Scmd_CMD_GAME_INPUT_REQ {
			input := m.GetInputReq().GetInput()
			room.AddUserInput(input)
		}
	case lockstep.LEAVE:
		room.AddLeave(task.U)
	case lockstep.OUT:
		if room != nil {
			roomid := room.Roomid
			lockstep.RoomMgr_g.RemoveRoom(roomid)
		}
	}
}

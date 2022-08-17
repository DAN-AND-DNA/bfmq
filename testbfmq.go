package main

import (
	"bfmq/msg"
	//"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
	"log"
	"strings"
	"time"
)

type Agent struct {
	Conn       *websocket.Conn
	ReadQueue  chan *msg.C2S
	SendQueue  chan *msg.C2S
	TimerQueue chan func()
	SendDone   chan struct{}
}

func NewAgent() {
	conn, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1:7737/", nil)
	if err != nil {
		log.Printf("connected err:%v", err)
		return
	} else {
		log.Println("connected success")
	}
	a := &Agent{
		Conn:       conn,
		ReadQueue:  make(chan *msg.C2S, 1000),
		SendQueue:  make(chan *msg.C2S, 1000),
		TimerQueue: make(chan func(), 1000),
		SendDone:   make(chan struct{}, 3),
	}

	go func() {
		for {
			op, data, err := a.Conn.ReadMessage()
			if err != nil {
				log.Printf("err:%v", err)
				if strings.Contains(err.Error(), "EOF") {
					a.Conn.Close()
					a.SendDone <- struct{}{}
					return
				} else if strings.Contains(err.Error(), "read tcp") {
					a.Conn.Close()
					a.SendDone <- struct{}{}
					return
				} else {
					a.Conn.Close()
					a.SendDone <- struct{}{}
					return
				}
			}

			if op == websocket.BinaryMessage {
				m := &msg.C2S{}
				if err := proto.Unmarshal(data, m); err == nil {
					a.ReadQueue <- m
				} else {
					log.Printf("proto err:%v", err)
					a.Conn.Close()
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case m := <-a.SendQueue:
				buffer, _ := proto.Marshal(m)
				err := a.Conn.WriteMessage(websocket.BinaryMessage, buffer)

				if err != nil {
					log.Printf("err:%v", err)
					a.Conn.Close()
					return
				}
			case <-a.SendDone:
				log.Println("send done")
				return
			}
		}
	}()

	go func() {

		go func() {
			for {
				a.TimerQueue <- func() {
					t := msg.C2Stype_TYPE_MAIN
					cmd0 := msg.C2Scmd_CMD_LOGIN_HEARTBEAT_REQ
					m0 := &msg.C2S{
						Head:         &msg.C2SHead{Type: &t, Seq: proto.Uint32(100), Cmd: &cmd0},
						Result:       &msg.C2SRes{},
						HeartBeatReq: &msg.HeartBeatReq{},
					}
					log.Printf("sed cmd: %v", cmd0)
					a.Send(m0)

				}
				time.Sleep(8 * time.Second)
			}
		}()

		t := msg.C2Stype_TYPE_MAIN
		cmd1 := msg.C2Scmd_CMD_LOGIN_GET_OPENID_REQ
		m1 := &msg.C2S{
			Head:         &msg.C2SHead{Type: &t, Seq: proto.Uint32(100), Cmd: &cmd1},
			Result:       &msg.C2SRes{},
			GetOpenidReq: &msg.GetOpenidReq{Code: proto.String("wxtestcode")},
		}

		log.Printf("sed cmd: %v", cmd1)
		a.Send(m1)
		for {
			select {
			case m := <-a.ReadQueue:
				if m.GetHead().GetCmd() != msg.C2Scmd_CMD_GAME_INPUT_NOTIFY {
					log.Printf("get cmd: %v", m.GetHead().GetCmd())
				}
				switch m.GetHead().GetCmd() {
				case msg.C2Scmd_CMD_LOGIN_GET_OPENID_RES:
					cmd2 := msg.C2Scmd_CMD_LOGIN_GET_DATA_REQ
					m2 := &msg.C2S{
						Head:       &msg.C2SHead{Type: &t, Seq: proto.Uint32(100), Cmd: &cmd2},
						Result:     &msg.C2SRes{},
						GetDataReq: &msg.GetDataReq{},
					}
					openid := m.GetGetOpenidRes().GetOpenid()
					m2.GetDataReq.Openid = (proto.String)(openid)
					log.Printf("sed cmd: %v", cmd2)
					a.Send(m2)
				case msg.C2Scmd_CMD_LOGIN_GET_DATA_RES:
					nickname := m.GetGetDataRes().GetPlayer().GetNickname()
					log.Printf("get nickname: %v", nickname)
					cmd3 := msg.C2Scmd_CMD_GAME_JOINROOM_REQ
					m3 := &msg.C2S{
						Head:        &msg.C2SHead{Type: &t, Seq: proto.Uint32(100), Cmd: &cmd3},
						Result:      &msg.C2SRes{},
						JoinRoomReq: &msg.JoinRoomReq{},
					}
					log.Printf("sed cmd: %v", cmd3)
					a.Send(m3)
				case msg.C2Scmd_CMD_GAME_JOINROOM_RES:
					roomid := m.GetJoinRoomRes().GetRoomid()
					log.Printf("join roomid: %v", roomid)
				case msg.C2Scmd_CMD_GAME_ENTERGAME_NOTIFY:
					roomid := m.GetEnterGameNotify().GetRoomid()
					cmd4 := msg.C2Scmd_CMD_GAME_READY_REQ
					m4 := &msg.C2S{
						Head:     &msg.C2SHead{Type: &t, Seq: proto.Uint32(100), Cmd: &cmd4},
						Result:   &msg.C2SRes{},
						ReadyReq: &msg.ReadyReq{},
					}

					m4.ReadyReq.Roomid = (proto.Uint32)(roomid)
					log.Printf("sed cmd: %v", cmd4)
					a.Send(m4)
				case msg.C2Scmd_CMD_GAME_STARTGAME_NOTIFY:
					go func() {
						for {
							time.Sleep(25 * time.Millisecond)
							a.TimerQueue <- func() {
								t := msg.C2Stype_TYPE_MAIN
								cmd0 := msg.C2Scmd_CMD_GAME_INPUT_REQ
								m0 := &msg.C2S{
									Head:     &msg.C2SHead{Type: &t, Seq: proto.Uint32(100), Cmd: &cmd0},
									Result:   &msg.C2SRes{},
									InputReq: &msg.InputReq{Roomid: (proto.Uint32)(1001), Input: &msg.InputData{}},
								}
								//log.Printf("sed cmd: %v", cmd0)
								a.Send(m0)

							}
						}
					}()
				case msg.C2Scmd_CMD_GAME_INPUT_NOTIFY:
					step := m.GetInputNotify().GetStep()
					log.Printf("get step: %d", step)

					if step >= 3600 {
						return
					}
					//time.Sleep(10 * time.Millisecond)
					t := msg.C2Stype_TYPE_MAIN
					cmd0 := msg.C2Scmd_CMD_GAME_INPUT_REQ
					m0 := &msg.C2S{
						Head:     &msg.C2SHead{Type: &t, Seq: proto.Uint32(100), Cmd: &cmd0},
						Result:   &msg.C2SRes{},
						InputReq: &msg.InputReq{Roomid: (proto.Uint32)(1001), Input: &msg.InputData{}},
					}
					//log.Printf("sed cmd: %v", cmd0)
					a.Send(m0)

				case msg.C2Scmd_CMD_LOGIN_HEARTBEAT_RES:
				}
			case f := <-a.TimerQueue:
				f()

			}
		}
	}()

}

func (this *Agent) Close() {
	this.Conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
	time.Sleep(2 * time.Second)
	this.Conn.Close()
}

func (this *Agent) Send(m *msg.C2S) {
	this.SendQueue <- m
}

func main() {
	NewAgent()
	time.Sleep(time.Second * 10000)

	//var conns []*websocket.Conn

	/*
		for i := 0; i < 1; i++ {
			c, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1:7737/", nil)
			if err != nil {
				break
			}
			log.Println("connected")

		//	conns = append(conns, c)
			defer func() {
				c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
				time.Sleep(time.Second)
				c.Close()
			}()
		}
	*/

	/*
		t := msg.C2Stype_TYPE_MAIN
		cmd1 := msg.C2Scmd_CMD_LOGIN_GET_OPENID_REQ
		m1 := &msg.C2S{
			Head:         &msg.C2SHead{Type: &t, Seq: proto.Uint32(100), Cmd: &cmd1},
			Result:       &msg.C2SRes{},
			GetOpenidReq: &msg.GetOpenidReq{Code: proto.String("wxtestcode")},
		}

		cmd2 := msg.C2Scmd_CMD_LOGIN_GET_DATA_REQ
		m2 := &msg.C2S{
			Head:       &msg.C2SHead{Type: &t, Seq: proto.Uint32(100), Cmd: &cmd2},
			Result:     &msg.C2SRes{},
			GetDataReq: &msg.GetDataReq{},
		}
		cmd3 := msg.C2Scmd_CMD_LOGIN_HEARTBEAT_REQ
		m3 := &msg.C2S{
			Head:         &msg.C2SHead{Type: &t, Seq: proto.Uint32(100), Cmd: &cmd3},
			Result:       &msg.C2SRes{},
			HeartBeatReq: &msg.HeartBeatReq{},
		}
		//	for i := 0; i < len(conns); i++ {
		//time.Sleep(1 * time.Second)
		i := 0
		conn := conns[i]
		//log.Printf("conn %d sending message", i)
		//	heartbeat(conn)
		buffer1, _ := proto.Marshal(m1)
		log.Printf("send cmd:%v", cmd1)
		conn.WriteMessage(websocket.BinaryMessage, buffer1)
		_, readbuffer, _ := conn.ReadMessage()
		resmsg := &msg.C2S{}
		_ = proto.Unmarshal(readbuffer, resmsg)

		res := resmsg.GetResult().GetRes()
		if res != -1 {
			openid := resmsg.GetGetOpenidRes().GetOpenid()
			log.Printf("get openid: %v", openid)
			m2.GetDataReq.Openid = (proto.String)(openid)
			buffer2, _ := proto.Marshal(m2)

			time.Sleep(1 * time.Second)

			log.Printf("send cmd:%v", cmd2)
			conn.WriteMessage(websocket.BinaryMessage, buffer2)
			_, readbuffer, _ := conn.ReadMessage()
			_ = proto.Unmarshal(readbuffer, resmsg)
			res = resmsg.GetResult().GetRes()
			if res != -1 {
				nickname := resmsg.GetGetDataRes().GetPlayer().GetNickname()
				log.Printf("get nickname: %v", nickname)
			}
		}
		//conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("hello dan %v", i)))
		//		}
		buffer3, _ := proto.Marshal(m3)
		for {
			time.Sleep(20 * time.Second)
			conn.WriteMessage(websocket.BinaryMessage, buffer3)
			conn.ReadMessage()
		}
	*/
}

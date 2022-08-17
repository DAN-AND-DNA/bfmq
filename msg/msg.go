package msg

import (
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/golang/protobuf/proto"
	"log"
	"net"
	"strings"
)

var TimerQueue chan func()
var MainReqQueue chan *Task
var MainResQueue chan struct{}

func init() {
	TimerQueue = make(chan func(), 10000)
	MainReqQueue = make(chan *Task, 1000000)
	MainResQueue = make(chan struct{}, 1000)
}

const (
	NEW uint8 = iota + 1
	MSG
	CLOSE
)

type Agent struct {
	Conn      net.Conn
	SendQueue chan *C2S
	SendDone  chan struct{}
	UserData  interface{}
}

type Task struct {
	Cmd uint8
	A   *Agent
	Msg *C2S
}

func NewAgent(conn net.Conn) {
	a := &Agent{
		Conn:      conn,
		SendQueue: make(chan *C2S, 1000),
		SendDone:  make(chan struct{}, 3),
	}

	MainReqQueue <- &Task{Cmd: NEW, A: a}

	go func() {
		for {
			data, op, err := wsutil.ReadClientData(a.Conn) // block here
			if err != nil {
				log.Printf("err:%v", err)
				if strings.Contains(err.Error(), "EOF") {
					a.close1()
					return
				} else if strings.Contains(err.Error(), "read tcp") {
					a.close1()
					return
				} else {
					a.close1()
					return
				}
			}
			if op == ws.OpBinary {
				m := &C2S{}
				if err := proto.Unmarshal(data, m); err == nil {
					MainReqQueue <- &Task{Cmd: MSG, A: a, Msg: m}
				} else {
					log.Printf("proto err:%v", err)
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case m := <-a.SendQueue:
				buffer, _ := proto.Marshal(m)
				err := wsutil.WriteServerMessage(a.Conn, ws.OpBinary, buffer)
				if err != nil {
					a.Conn.Close()
					return
				}
			case <-a.SendDone:
				log.Println("send done")
				return
			}
		}
	}()

}

func (this *Agent) Send(m *C2S) {
	if this == nil {
		return
	}

	this.SendQueue <- m
}

func (this *Agent) close1() {

	log.Printf("close1")
	if this == nil {
		return
	}

	if this.Conn != nil {
		MainReqQueue <- &Task{Cmd: CLOSE, A: this}
		this.SendDone <- struct{}{}
		this.Conn.Close() // make blocking op(read or write) go on
	}
}

func (this *Agent) Close() {
	log.Printf("close")
	if this == nil {
		return
	}

	if this.Conn != nil {
		this.Conn.Close() // make blocking op(like read or write) go on
	}
}

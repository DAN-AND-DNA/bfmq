package main

import (
	"bfmq/lib/lockstep"
	"bfmq/lib/tinypool"
	"bfmq/mod"
	"bfmq/msg"
	"github.com/gobwas/ws"
	"log"
	"net"
	"runtime"
	"syscall"
	"time"
)

const (
	NEW uint8 = iota + 1
	MSG
	CLOSE
)

type Task struct {
	Cmd uint8
	A   *msg.Agent
	Msg *msg.C2S
}

func tick() {
	tinypool.KickIdles()
	tinypool.Time2PVE()
	tinypool.Time2Play()

}

func ticker() {
	msg.TimerQueue <- tick
	time.AfterFunc(1*time.Second, ticker)
}

func init() {
	tinypool.PreRun(15, 8, 5)
	//for fd
	log.Println("optimize linux soft limit......")
	var stRlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &stRlimit); err != nil {
		panic(err)
	}

	stRlimit.Cur = stRlimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &stRlimit); err != nil {
		panic(err)
	}
	log.Printf("optimize linux success:%d", stRlimit.Cur)

	//numCPU := 0
	numCPU := runtime.NumCPU()

	log.Printf("set available CPU core:%d", numCPU)
	runtime.GOMAXPROCS(numCPU)

	log.Println("init success")
}

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:7737")
	if err != nil {
		return
	}

	time.AfterFunc(1*time.Second, ticker)
	go maingo()
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept failed:%v", err)
			if conn != nil {
				conn.Close()
			}
			continue
		}

		_, err = ws.Upgrade(conn)
		if err != nil {
			log.Printf("websocket upgrade failed:%v", err)
			if conn != nil {
				conn.Close()
			}
			continue
		}
		msg.NewAgent(conn)
	}
}

func maingo() {
	for {
		select {
		case task := <-msg.MainReqQueue:
			mod.HandleMsg(task)
		case task := <-lockstep.RoomOutQueue:
			mod.HandleRoom(task)
		case f := <-msg.TimerQueue:
			if f != nil {
				f()
			}
		}
	}
}

/*
func roomgo() {
	for {
		select {
		case task := <-lockstep.RoomInQueue:
			mod.HandleRoom(task)
		}
	}
}
*/

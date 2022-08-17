package lockstep

import (
	"bfmq/lib/game"
	"bfmq/msg"
	"errors"
	"log"
)

type PLAYERSTATE uint8

const (
	QUEUEED PLAYERSTATE = iota + 100
	ALREADY
	PLAYING
)

type PlayerRoom struct {
	Agent     *msg.Agent
	Playerid  uint32
	State     PLAYERSTATE
	Nickname  string
	AvatarUrl string
	Userid    uint64
}

var testconfig []string

func init() {
	testconfig = append(testconfig, "你的小姐姐鸭", "一袋天椒", "你打球像CXK", "吓得我投翔", "薇可馨", "千纸鹤", "杜冷丁", "玺欢你", "白月光", "左先生", "小娇妻", "秋意凉", "创可贴", "有时候", "臭屁烊", "烊崽儿", "夙主公", "小傻瓜", "易夫人", "薄荷糖", "再拥有", "小姐姐", "世俗姬", "梁思彤", "有点甜", "苏沫颜", "美人殇", "世界那么大", "捣蛋小天使", "盖世小仙女", "甜心草霉味", "病态系少女", "仙女有只猫", "一世苏渃瘾", "雾里看小花", "傲娇你淼爷", "雨辰的烊光", "对面的女孩", "易生玺爱烊", "短路一秒钟", "日落说再见", "永远萤火虫", "大佬的二十", "心事让风听", "孤单荒凉了", "囚鱼不知天", "主要看气质", "迁就你一生", "最爱玺子哥", "诗酒趁年华", "时光如初见", "病态系少女", "患上汉子癌", "心有所坤", "衡文清君", "瞌睡少女", "萍颖如梦", "夏之炫音", "续写思源", "七分微笑", "两星相悦", "李月天地", "浅夏微霜", "桂颖花骨", "玫瑰花雨", "蔷微传奇", "最明显的伤心", "不可触碰的伤", "特别关心可好", "月光下的你我", "迷人不及你亚", "喵星萌爪来袭", "给颗糖就不哭", "爱笑的女孩儿", "冰柠檬的冬天", "该用户已成仙", "谁的青涩芒果", "被丢弃的女孩", "最凉不过人心", "最普通的女人", "暖暖的陌生人", "你若在我便爱", "遥不可及的梦", "恰逢时光如城", "越努力越幸运", "雨中摇曳的荷", "等你来爱你我")
}

func NewPlayerRoom(agent *msg.Agent, playerid uint32, seed uint32) *PlayerRoom {

	if agent != nil {
		// MAN
		p := agent.UserData
		playergame, ok := p.(*game.PlayerGame)
		if ok == true {
		} else {
			return nil
		}

		pr := &PlayerRoom{
			Agent:    agent,
			Playerid: playerid,
			State:    QUEUEED,
		}
		pr.AvatarUrl = playergame.PlayerDB.GetAvatarUrl()
		pr.Nickname = playergame.PlayerDB.GetNickname()
		pr.Userid = playergame.PlayerDB.GetUserid()
		log.Printf("man join room  nickname:%v, url:%v", pr.Nickname, pr.AvatarUrl)
		return pr
	} else {
		// AI
		pr := &PlayerRoom{
			Agent:    agent,
			Playerid: playerid,
			State:    QUEUEED,
		}

		ai := (seed+playerid)%(uint32)(len(testconfig)-1) + 1
		pr.AvatarUrl = "http://bf.mycdn.cn/ai37"
		pr.Nickname = testconfig[ai]
		log.Printf("ai join room  nickname:%v, url:%v", pr.Nickname, pr.AvatarUrl)
		return pr
	}
}

func (this *PlayerRoom) SendMsg(msg *msg.C2S) {
	if this.Agent != nil {
		this.Agent.Send(msg)
	}
}

func (this *PlayerRoom) IsOnline() bool {
	if this.Agent != nil {
		return true
	} else {
		return false
	}
}

func (this *PlayerRoom) GetUserid() (uint64, error) {
	if this.Agent != nil {
		return this.Userid, nil
	} else {
		return 0, errors.New("the player is offline")
	}
}

func (this *PlayerRoom) SetStatus(s PLAYERSTATE) {
	this.State = s
}

func (this *PlayerRoom) GetStatus() PLAYERSTATE {
	return this.State
}

func (this *PlayerRoom) LostConnection() {
	this.Agent = nil
}

func (this *PlayerRoom) ReConnection(agent *msg.Agent) {
	this.Agent = agent
}

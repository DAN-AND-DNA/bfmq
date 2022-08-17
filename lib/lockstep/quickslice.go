package lockstep

import (
	"bfmq/msg"
	"sync"
)

type QuickSlice struct {
	sync.Mutex
	Len  int
	Cap  int
	data []*msg.InputData
}

func NewQuickSlice(len int, cap int) *QuickSlice {
	return &QuickSlice{
		Len:  len,
		Cap:  cap,
		data: make([]*msg.InputData, len, cap),
	}
}

func (this *QuickSlice) Append(input *msg.InputData) {
	this.data = append(this.data, input)
}

func (this *QuickSlice) Appendm(input *msg.InputData) {
	this.Lock()
	defer this.Unlock()

	this.data = append(this.data, input)
}

func (this *QuickSlice) GetAll() (d []*msg.InputData) {
	len := len(this.data)

	if len > 0 {
		d, this.data = this.data[:len], make([]*msg.InputData, this.Len, this.Cap)
	}
	return
}

func (this *QuickSlice) GetAllm() (d []*msg.InputData) {
	this.Lock()
	defer this.Unlock()

	len := len(this.data)

	if len > 0 {
		d, this.data = this.data[:len], make([]*msg.InputData, this.Len, this.Cap)
	}
	return
}

//----------------------

type QuickSlice1 struct {
	sync.Mutex
	Len  int
	Cap  int
	data []uint64
}

func NewQuickSlice1(len int, cap int) *QuickSlice1 {
	return &QuickSlice1{
		Len:  len,
		Cap:  cap,
		data: make([]uint64, len, cap),
	}
}

func (this *QuickSlice1) Append(input uint64) {
	this.Lock()
	defer this.Unlock()

	this.data = append(this.data, input)
}

func (this *QuickSlice1) GetAll() (d []uint64) {
	this.Lock()
	defer this.Unlock()

	len := len(this.data)

	if len > 0 {
		d, this.data = this.data[:len], make([]uint64, this.Len, this.Cap)
	}
	return
}

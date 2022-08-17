/*  author: dan
    github: https://github.com/DAN-AND-DNA
    v0.01 : dan 2019-03-09
*/

package tinywheel

type Entry struct {
	Conns map[interface{}]interface{}
	Num   uint16
	Next  *Entry
}

type TinyWheel struct {
	MaxTime uint16
	Tail    *Entry
	Head    *Entry
	Chucks  []*Entry
}

func (this *TinyWheel) Size() uint32 {
	ptr := this.Head
	size := 0

	for {
		size += len(ptr.Conns)
		ptr = ptr.Next
		if ptr == this.Head {
			break
		}
	}
	return (uint32)(size)
}

func (this *TinyWheel) Del(conn interface{}, num uint16) {
	pst := this.Chucks[num]
	delete(pst.Conns, conn)

}

// loop in tick
func (this *TinyWheel) Loop() *map[interface{}]interface{} {
	this.Tail = this.Tail.Next
	expired := this.Tail.Conns

	this.Tail.Conns = make(map[interface{}]interface{})
	return &expired
}

func (this *TinyWheel) Pop(num uint16, key interface{}) (interface{}, bool) {
	if pst := this.Chucks[num]; pst == nil {
		return nil, false
	} else {
		if v, ok := pst.Conns[key]; ok {
			this.Del(key, num)
			return v, ok
		} else {
			return nil, false
		}
	}
}

func (this *TinyWheel) Add(conn interface{}, times interface{}) uint16 {
	this.Tail.Conns[conn] = times
	return this.Tail.Num
}

func (this *TinyWheel) Keep(conn interface{}, num uint16, times uint8) uint16 {
	this.Del(conn, num)
	return this.Add(conn, times)
}

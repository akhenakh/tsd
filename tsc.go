package tsd

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/golang/geo/s2"
)

const meterLevel = 23

type TimeSeries struct {
	// Format is as follow
	// header ts uint32,  cellID
	//     32               64

	// time delta,   cell delta
	//    16,        64
	b []byte

	// starting date
	t0 uint32

	// current
	t uint32
	c uint64
}

type Iter struct {
	ts *TimeSeries
	i  uint

	// current
	t uint32
	c uint64
}

func New() *TimeSeries {
	return &TimeSeries{}
}

// Push a ts, a value and uint64 cell representation
func (ts *TimeSeries) Push(t uint32, lat, lng float64) {
	ll := s2.LatLngFromDegrees(lat, lng)
	c := uint64(s2.CellIDFromLatLng(ll).Parent(meterLevel))

	// simply write as is
	if len(ts.b) == 0 {
		b := make([]byte, 12)
		binary.BigEndian.PutUint32(b, t)
		binary.BigEndian.PutUint64(b[4:], c)
		ts.b = b
		ts.t0 = t
		ts.t = t
		ts.c = c
		return
	}

	b := make([]byte, 10)
	tDelta := uint16(t - ts.t)
	binary.BigEndian.PutUint16(b, tDelta)
	//cDelta := ts.c - c
	//binary.BigEndian.PutUint64(b[2:], cDelta)
	binary.BigEndian.PutUint64(b[2:], c)
	//fmt.Printf("push tDelta %d cDelta %d hex %s\ncell: %d old cell: %d\n", tDelta, cDelta, hex.EncodeToString(b), c, ts.c)
	fmt.Printf("push tDelta %d c %d hex %s\ncell: %d old cell: %d\n", tDelta, c, hex.EncodeToString(b), c, ts.c)
	ts.t = t
	ts.c = c
	ts.b = append(ts.b, b...)
}

func (ts *TimeSeries) MarshalBinary() ([]byte, error) {
	return ts.b, nil
}

func (ts *TimeSeries) Iter() *Iter {
	return &Iter{ts: ts}
}

func (itr *Iter) Next() bool {
	if len(itr.ts.b) < 12 {
		return false
	}

	// read header
	if itr.i == 0 {
		itr.t = binary.BigEndian.Uint32(itr.ts.b)
		itr.c = binary.BigEndian.Uint64(itr.ts.b[4:])
		itr.i = 12
		return true
	}

	if itr.i+10 > uint(len(itr.ts.b)) {
		return false
	}

	//fmt.Println("DEBUG Read", hex.EncodeToString(itr.ts.b[itr.i+2:itr.i+10]))

	tDelta := binary.BigEndian.Uint16(itr.ts.b[itr.i:])
	itr.t += uint32(tDelta)
	//fmt.Println("DEBUG val", tDelta, itr.t)

	//cDelta := binary.BigEndian.Uint64(itr.ts.b[itr.i+2:])
	//itr.c = itr.c - cDelta
	itr.c = binary.BigEndian.Uint64(itr.ts.b[itr.i+2:])
	itr.i += 10
	return true
}

// Values returns ts, lat, lng
func (itr *Iter) Values() (uint32, float64, float64) {
	ll := s2.CellID(itr.c).LatLng()

	return itr.t, ll.Lat.Degrees(), ll.Lng.Degrees()
}

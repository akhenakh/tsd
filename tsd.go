package tsd

import (
	"bytes"
	"encoding/binary"
	"math"
)

// TimeSeries handles compressed in memory storage
type TimeSeries struct {
	// Format is as follow
	// header ts uint32,   lat float32, lng float32
	//     32                  32           32

	//  info,  time delta dyn,   lat dyn, lng dyn
	//   1          n,                   n,      n

	// in memory compressed storage
	// note the cap of this slice is manage by append()
	b []byte

	// current
	t                  uint32
	tdelta             uint32
	tdod               int32
	lat, lng           int32
	latdod, lngdod     int32
	latdelta, lngdelta int32
}

// Iter to iterate other the timeseries values
type Iter struct {
	ts *TimeSeries
	i  uint

	// current
	t                  uint32
	tdelta             uint32
	lat, lng           int32
	latdelta, lngdelta int32
}

// New returns a new timeseries
func New() *TimeSeries {
	return &TimeSeries{}
}

// Push a ts and lat lng
func (ts *TimeSeries) Push(t uint32, lat, lng float32) {
	// simply write as is
	if len(ts.b) == 0 {
		ts.writeHeader(t, lat, lng)
		return
	}

	// at most it will take 1 + 12
	buf := new(bytes.Buffer)

	var denc DeltaEncoding

	// encoding TS
	// checking for the 1st entry case
	if len(ts.b) == 4+4+4 {
		ts.tdelta = t - ts.t
		ts.tdod = int32(ts.tdelta)
	} else {
		ndelta := t - ts.t
		ts.tdod = int32(ndelta - ts.tdelta)
		ts.tdelta = ndelta
	}
	ts.t = t

	switch {
	case ts.tdod == 0:
		denc = Delta0
	case ts.tdod <= math.MaxInt8 && ts.tdod >= math.MinInt8:
		denc = Delta8
		tDelta8 := int8(ts.tdod)
		binary.Write(buf, binary.BigEndian, tDelta8)
	case ts.tdod <= math.MaxInt16 && ts.tdod >= math.MinInt16:
		denc = Delta16
		tDelta16 := int16(ts.tdod)
		binary.Write(buf, binary.BigEndian, tDelta16)
	default:
		denc = Full32
		binary.Write(buf, binary.BigEndian, t)
		ts.tdod = 0
		ts.tdelta = 0
	}

	// encoding latitude
	ilat := int32(math.Round(float64(lat) * 100_000))
	// checking for the 1st entry case
	if len(ts.b) == 4+4+4 {
		ts.latdelta = ilat - ts.lat
		ts.latdod = ts.latdelta
	} else {
		ndelta := ilat - ts.lat
		ts.latdod = ndelta - ts.latdelta
		ts.latdelta = ndelta
	}
	ts.lat = ilat

	switch {
	case ts.latdod == 0:
		denc ^= Delta0 << 2
	case ts.latdod <= math.MaxInt8 && ts.latdod >= math.MinInt8:
		denc ^= Delta8 << 2
		latDelta8 := int8(ts.latdod)
		binary.Write(buf, binary.BigEndian, latDelta8)
	case ts.latdod <= math.MaxInt16 && ts.latdod >= math.MinInt16:
		denc ^= Delta16 << 2
		latDelta16 := int16(ts.latdod)
		binary.Write(buf, binary.BigEndian, latDelta16)
	default:
		denc ^= Full32 << 2
		ts.latdod = 0
		ts.latdelta = 0
		binary.Write(buf, binary.BigEndian, ilat)
	}

	// encoding longitude
	ilng := int32(math.Round(float64(lng) * 100_000))
	// checking for the 1st entry case
	if len(ts.b) == 4+4+4 {
		ts.lngdelta = ilng - ts.lng
		ts.lngdod = ts.lngdelta
	} else {
		ndelta := ilng - ts.lng
		ts.lngdod = ndelta - ts.lngdelta
		ts.lngdelta = ndelta
	}
	ts.lng = ilng

	switch {
	case ts.lngdod == 0:
		denc ^= Delta0 << 4
	case ts.lngdod <= math.MaxInt8 && ts.lngdod >= math.MinInt8:
		denc ^= Delta8 << 4
		lngDelta8 := int8(ts.lngdod)
		binary.Write(buf, binary.BigEndian, lngDelta8)
	case ts.lngdod <= math.MaxInt16 && ts.lngdod >= math.MinInt16:
		denc ^= Delta16 << 4
		lngDelta16 := int16(ts.lngdod)
		binary.Write(buf, binary.BigEndian, lngDelta16)
	default:
		denc ^= Full32 << 4
		ts.lngdod = 0
		ts.lngdelta = 0
		binary.Write(buf, binary.BigEndian, ilng)
	}

	ts.b = append(ts.b, byte(denc))
	ts.b = append(ts.b, buf.Bytes()...)
}

func (ts *TimeSeries) writeHeader(t uint32, lat float32, lng float32) {
	b := make([]byte, 12)
	binary.BigEndian.PutUint32(b, t)
	ts.lat = int32(math.Round(float64(lat) * 100_000))
	ts.lng = int32(math.Round(float64(lng) * 100_000))
	binary.BigEndian.PutUint32(b[4:], uint32(ts.lat))
	binary.BigEndian.PutUint32(b[8:], uint32(ts.lng))
	ts.b = b
	ts.t = t
}

// MarshalBinary marshal into binary for cold storage
func (ts *TimeSeries) MarshalBinary() ([]byte, error) {
	return ts.b, nil
}

// UnmarshalBinary unmarshal from cold storage into a live in memory timeseries
func (ts *TimeSeries) UnmarshalBinary(data []byte) error {
	ts.b = data
	itr := ts.Iter()
	for itr.Next() {
	}
	return nil
}

// Iter returns a new iterator from the beginning
func (ts *TimeSeries) Iter() *Iter {
	return &Iter{ts: ts}
}

// Next returns true if there is still a value available
func (itr *Iter) Next() bool {
	if len(itr.ts.b) < 12 {
		return false
	}

	// read header
	if itr.i == 0 {
		buf := bytes.NewReader(itr.ts.b)
		var err error
		read := func(data interface{}) {
			if err != nil {
				return
			}
			err = binary.Read(buf, binary.BigEndian, data)
		}
		read(&itr.t)
		read(&itr.lat)
		read(&itr.lng)
		if err != nil {
			return false
		}
		itr.i = 12
		return true
	}

	// the minimum viable size is 1B
	if itr.i+1 > uint(len(itr.ts.b)) {
		return false
	}

	denc := DeltaEncoding(itr.ts.b[itr.i])
	itr.i++

	// it's probably a bogus entry
	if denc > 0b111111 {
		return false
	}

	var dod int32

	switch denc.TSDelta() {
	case Delta0:
		dod = 0
	case Delta8:
		dod = int32(int8(itr.ts.b[itr.i]))
		itr.i++
	case Delta16:
		dod = int32(int16(binary.BigEndian.Uint16(itr.ts.b[itr.i:])))
		itr.i += 2
	case Full32:
		itr.t = binary.BigEndian.Uint32(itr.ts.b[itr.i:])
		itr.i += 4
		itr.tdelta = 0
		dod = 0
	}

	itr.t += uint32(int32(itr.tdelta) + dod)
	itr.tdelta = uint32(int32(itr.tdelta) + dod)

	var dodCoord int32

	switch denc.LatDelta() {
	case Delta0:
		dodCoord = 0
	case Delta8:
		dodCoord = int32(int8(itr.ts.b[itr.i]))
		itr.i++
	case Delta16:
		dodCoord = int32(int16(binary.BigEndian.Uint16(itr.ts.b[itr.i:])))
		itr.i += 2
	case Full32:
		itr.lat = int32(binary.BigEndian.Uint32(itr.ts.b[itr.i:]))
		itr.i += 4
		itr.latdelta = 0
		dodCoord = 0
	}

	itr.lat += itr.latdelta + dodCoord
	itr.latdelta += dodCoord

	switch denc.LngDelta() {
	case Delta0:
		dodCoord = 0
	case Delta8:
		dodCoord = int32(int8(itr.ts.b[itr.i]))
		itr.i++
	case Delta16:
		dodCoord = int32(int16(binary.BigEndian.Uint16(itr.ts.b[itr.i:])))
		itr.i += 2
	case Full32:
		itr.lng = int32(binary.BigEndian.Uint32(itr.ts.b[itr.i:]))
		itr.i += 4
		itr.lngdelta = 0
		dodCoord = 0
	}

	itr.lng += itr.lngdelta + dodCoord
	itr.lngdelta += dodCoord

	return true
}

// Values returns ts, lat, lng
func (itr *Iter) Values() (uint32, float32, float32) {
	return itr.t, float32(itr.lat) / 100_000, float32(itr.lng) / 100_000
}

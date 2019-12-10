package tsd

import (
	"bytes"
	"encoding/binary"
	"math"
)

type TimeSeries struct {
	// Format is as follow
	// header ts uint32,   lat float32, lng float32
	//     32                  32           32

	//  info,  time delta dyn,   lat dyn, lng dyn
	//   1          n,                   n,      n
	b []byte

	// current
	t                  uint32
	tdelta             uint32
	tdod               int32
	lat, lng           int32
	latdod, lngdod     int32
	latdelta, lngdelta int32
}

type Iter struct {
	ts *TimeSeries
	i  uint

	// current
	t                  uint32
	tdelta             uint32
	lat, lng           int32
	latdelta, lngdelta int32
}

func New() *TimeSeries {
	return &TimeSeries{}
}

// Push a ts and lat lng
func (ts *TimeSeries) Push(t uint32, lat, lng float32) {
	// simply write as is
	if len(ts.b) == 0 {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, t)
		ts.lat = int32(math.Round(float64(lat) * 100_000))
		ts.lng = int32(math.Round(float64(lng) * 100_000))
		binary.Write(buf, binary.BigEndian, ts.lat)
		binary.Write(buf, binary.BigEndian, ts.lng)

		ts.b = buf.Bytes()
		ts.t = t
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
		denc = TSDelta0
	case ts.tdod <= math.MaxInt8 && ts.tdod >= math.MinInt8:
		denc = TSDelta8
		tDelta8 := int8(ts.tdod)
		binary.Write(buf, binary.BigEndian, tDelta8)
	case ts.tdod <= math.MaxInt16 && ts.tdod >= math.MinInt16:
		denc = TSDelta16
		tDelta16 := int16(ts.tdod)
		binary.Write(buf, binary.BigEndian, tDelta16)
	default:
		denc = TSFull32
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
		denc ^= LatDelta0 << 2
	case ts.latdod <= math.MaxInt8 && ts.latdod >= math.MinInt8:
		denc ^= LatDelta8 << 2
		latDelta8 := int8(ts.latdod)
		binary.Write(buf, binary.BigEndian, latDelta8)
	case ts.latdod <= math.MaxInt16 && ts.latdod >= math.MinInt16:
		denc ^= LatDelta16 << 2
		latDelta16 := int16(ts.latdod)
		binary.Write(buf, binary.BigEndian, latDelta16)
	default:
		denc ^= LatFull32 << 2
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

	//fmt.Printf("DEBUG encoding ts %d %d lat %d\t%d\t\t%d\t\tlng\t\t%d\t\t%d\t%d\n", ts.t, ts.tdelta, ilat, ts.latdelta, ts.latdod, ilng, ts.lngdelta, ts.lngdod)

	switch {
	case ts.lngdod == 0:
		denc ^= LngDelta0 << 4
	case ts.lngdod <= math.MaxInt8 && ts.lngdod >= math.MinInt8:
		denc ^= LngDelta8 << 4
		lngDelta8 := int8(ts.lngdod)
		binary.Write(buf, binary.BigEndian, lngDelta8)
	case ts.lngdod <= math.MaxInt16 && ts.lngdod >= math.MinInt16:
		denc ^= LngDelta16 << 4
		lngDelta16 := int16(ts.lngdod)
		binary.Write(buf, binary.BigEndian, lngDelta16)
	default:
		denc ^= LngFull32 << 4
		ts.lngdod = 0
		ts.lngdelta = 0
		binary.Write(buf, binary.BigEndian, ilng)
	}

	ts.b = append(ts.b, byte(denc))
	ts.b = append(ts.b, buf.Bytes()...)
}

func (ts *TimeSeries) MarshalBinary() ([]byte, error) {
	return ts.b, nil
}

func (ts *TimeSeries) UnmarshalBinary(data []byte) error {
	ts.b = data
	itr := ts.Iter()
	for itr.Next() {
	}
	return nil
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
		buf := bytes.NewReader(itr.ts.b)
		binary.Read(buf, binary.BigEndian, &itr.t)
		binary.Read(buf, binary.BigEndian, &itr.lat)
		binary.Read(buf, binary.BigEndian, &itr.lng)

		itr.i = 12
		return true
	}

	// the minimum viable size is 1B
	if itr.i+1 > uint(len(itr.ts.b)) {
		return false
	}

	denc := DeltaEncoding(itr.ts.b[itr.i])
	itr.i += 1

	var dod int32

	switch denc.TSDelta() {
	case TSDelta0:
		dod = 0
	case TSDelta8:
		dod = int32(int8(itr.ts.b[itr.i]))
		itr.i += 1
	case TSDelta16:
		dod = int32(int16(binary.BigEndian.Uint16(itr.ts.b[itr.i:])))
		itr.i += 2
	case TSFull32:
		itr.t = binary.BigEndian.Uint32(itr.ts.b[itr.i:])
		itr.i += 4
		itr.tdelta = 0
		dod = 0
	}

	itr.t += uint32(int32(itr.tdelta) + dod)
	itr.tdelta = uint32(int32(itr.tdelta) + dod)

	var dodCoord int32

	switch denc.LatDelta() {
	case LatDelta0:
		dodCoord = 0
	case LatDelta8:
		dodCoord = int32(int8(itr.ts.b[itr.i]))
		itr.i += 1
	case LatDelta16:
		dodCoord = int32(int16(binary.BigEndian.Uint16(itr.ts.b[itr.i:])))
		itr.i += 2
	case LatFull32:
		itr.lat = int32(binary.BigEndian.Uint32(itr.ts.b[itr.i:]))
		itr.i += 4
		itr.latdelta = 0
		dodCoord = 0
	}

	itr.lat += itr.latdelta + dodCoord
	itr.latdelta = itr.latdelta + dodCoord
	//fmt.Printf("DEBUG decoding ts %d %d lat %d\t%d\t\t%d\t\t", itr.t, itr.tdelta, itr.lat, itr.latdelta, dodCoord)

	switch denc.LngDelta() {
	case LngDelta0:
		dodCoord = 0
	case LngDelta8:
		dodCoord = int32(int8(itr.ts.b[itr.i]))
		itr.i += 1
	case LngDelta16:
		dodCoord = int32(int16(binary.BigEndian.Uint16(itr.ts.b[itr.i:])))
		itr.i += 2
	case LngFull32:
		itr.lng = int32(binary.BigEndian.Uint32(itr.ts.b[itr.i:]))
		itr.i += 4
		itr.lngdelta = 0
		dodCoord = 0
	}

	itr.lng += itr.lngdelta + dodCoord
	itr.lngdelta = itr.lngdelta + dodCoord
	//fmt.Printf("lng %d\t%d\t\t%d\n", itr.lng, itr.lngdelta, dodCoord)

	return true
}

// Values returns ts, lat, lng
func (itr *Iter) Values() (uint32, float32, float32) {
	return itr.t, float32(itr.lat) / 100_000, float32(itr.lng) / 100_000
}

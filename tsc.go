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

	// starting date
	t0 uint32

	// current
	t        uint32
	lat, lng int32
}

type Iter struct {
	ts *TimeSeries
	i  uint

	// current
	t        uint32
	lat, lng int32
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
		ts.t0 = t
		ts.t = t
		return
	}

	// at most it will take 1 + 12
	buf := new(bytes.Buffer)

	var denc DeltaEncoding

	// delta encoding TS
	tDelta := t - ts.t
	switch {
	case tDelta <= math.MaxUint8:
		denc = TSDelta8
		tDelta8 := uint8(t - ts.t)
		binary.Write(buf, binary.BigEndian, tDelta8)
	case tDelta <= math.MaxUint16:
		denc = TSDelta16
		tDelta16 := uint16(t - ts.t)
		binary.Write(buf, binary.BigEndian, tDelta16)
	default:
		denc = TSFull32
		binary.Write(buf, binary.BigEndian, t)
	}

	// delta encoding lat
	ilat := int64(math.Round(float64(lat) * 100_000))
	latDelta := ilat - int64(ts.lat)
	switch {
	case latDelta == 0:
		denc ^= LatDelta0 << 2
	case latDelta <= math.MaxInt8 && latDelta >= math.MinInt8:
		denc ^= LatDelta8 << 2
		latDelta8 := int8(latDelta)
		binary.Write(buf, binary.BigEndian, latDelta8)
	case latDelta <= math.MaxInt16 && latDelta >= math.MinInt16:
		denc ^= LatDelta16 << 2
		latDelta16 := int16(latDelta)
		binary.Write(buf, binary.BigEndian, latDelta16)
	default:
		denc ^= LatFull32 << 2
		binary.Write(buf, binary.BigEndian, int32(ilat))
	}

	// delta encoding lng
	ilng := int64(math.Round(float64(lng) * 100_000))
	lngDelta := ilng - int64(ts.lng)
	switch {
	case lngDelta == 0:
		denc ^= LngDelta0 << 4
	case lngDelta <= math.MaxInt8 && lngDelta >= math.MinInt8:
		denc ^= LngDelta8 << 4
		lngDelta8 := int8(lngDelta)
		binary.Write(buf, binary.BigEndian, lngDelta8)
	case lngDelta <= math.MaxInt16 && lngDelta >= math.MinInt16:
		denc ^= LngDelta16 << 4
		lngDelta16 := int16(lngDelta)
		binary.Write(buf, binary.BigEndian, lngDelta16)
	default:
		denc ^= LngFull32 << 4
		binary.Write(buf, binary.BigEndian, int32(ilng))
	}

	ts.t = t
	ts.lat += int32(latDelta)
	ts.lng += int32(lngDelta)
	ts.b = append(ts.b, byte(denc))
	ts.b = append(ts.b, buf.Bytes()...)
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

	switch denc.TSDelta() {
	case TSDelta8:
		itr.t += uint32(itr.ts.b[itr.i])
		itr.i += 1
	case TSDelta16:
		itr.t += uint32(binary.BigEndian.Uint16(itr.ts.b[itr.i:]))
		itr.i += 2
	case TSFull32:
		itr.t = binary.BigEndian.Uint32(itr.ts.b[itr.i:])
		itr.i += 4
	}

	switch denc.LatDelta() {
	case LatDelta8:
		itr.lat += int32(int8(itr.ts.b[itr.i]))
		itr.i += 1
	case LatDelta16:
		itr.lat += int32(int16(binary.BigEndian.Uint16(itr.ts.b[itr.i:])))
		itr.i += 2
	case LatFull32:
		itr.lat = int32(binary.BigEndian.Uint32(itr.ts.b[itr.i:]))
		itr.i += 4
	}

	switch denc.LngDelta() {
	case LngDelta8:
		itr.lng += int32(int8(itr.ts.b[itr.i]))
		itr.i += 1
	case LngDelta16:
		itr.lng += int32(int16(binary.BigEndian.Uint16(itr.ts.b[itr.i:])))
		itr.i += 2
	case LngFull32:
		itr.lng = int32(binary.BigEndian.Uint32(itr.ts.b[itr.i:]))
		itr.i += 4
	}
	return true
}

// Values returns ts, lat, lng
func (itr *Iter) Values() (uint32, float32, float32) {
	return itr.t, float32(itr.lat) / 100_000, float32(itr.lng) / 100_000
}

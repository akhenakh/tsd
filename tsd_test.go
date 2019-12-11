package tsd_test

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4"

	"github.com/akhenakh/tsd"
)

type Entry struct {
	Ts       uint32
	Lat, Lng float32
}

func TestCompareCompress(t *testing.T) {
	for _, filename := range taxiDataFiles()[:10] {
		file, err := os.Open(filename)
		if err != nil {
			t.Fatal(err)
		}
		b := readTSCoordAsFloats(file)
		fullSize := len(b)
		// Snappy
		sn := snappy.Encode(nil, b)
		snapSize := len(sn)
		// LZ4
		buf := make([]byte, len(b))
		ht := make([]int, 64<<10) // buffer for the compression table

		n, err := lz4.CompressBlock(b, buf, ht)
		if err != nil {
			log.Fatal(err)
		}
		if n >= len(b) {
			log.Fatal("can't compress ", filename)
		}
		buf = buf[:n] // compressed data
		lz4Size := len(buf)
		if lz4Size == 0 {
			lz4Size = 2 ^ 32
		}

		// tsd
		file.Seek(0, 0)
		entries := readTSCoordAsEntries(file)
		file.Close()

		ts := tsd.New()
		for _, e := range entries {
			ts.Push(e.Ts, e.Lat, e.Lng)
		}

		b, _ = ts.MarshalBinary()
		tsdSize := len(b)

		if lz4Size < tsdSize {
			t.Log("LZ4 better Compressed", lz4Size, len(b))
		}
		if snapSize < tsdSize {
			t.Log("Snappy better Compressed", snapSize, len(b))
		}

		t.Logf("File: %s\t Size: %d\tSnappy %d\tLZ4 %d\tTSD %d", filename, fullSize, snapSize, lz4Size, tsdSize)
	}
}

func TestUnmarshalBinary(t *testing.T) {
	b, _ := hex.DecodeString("47a4d541003ce61f00b1c7922a0258020504f528fd6c0b80")
	ts := tsd.New()
	err := ts.UnmarshalBinary(b)
	if err != nil {
		t.Fatal(err)
	}
	itr := ts.Iter()
	i := 0
	for itr.Next() {
		i++
		if i == 3 {
			ts, lat, lng := itr.Values()
			if ts != 1201986033 {
				t.Fatal("got invalid final ts")
			}
			if lat != 39.91445 {
				t.Fatal("got invalid final lng")
			}
			if lng != 116.56444 {
				t.Fatal("got invalid final lat")
			}
		}
	}
	if i != 3 {
		t.Fatal("expected 3 values")
	}
}

func TestDeltaOfDelta(t *testing.T) {
	ts := tsd.New()
	const aTime = uint32(1201986030)
	ts.Push(aTime, 48.82, 2.22)
	ts.Push(aTime+10, 48.83001, 2.23)
	ts.Push(aTime+20, 48.83002, 2.24)
	ts.Push(aTime+30, 48.83003, 2.25)
	ts.Push(aTime+41, 48.83004, 2.26)
	// we also want to test a time with a diff larger than max uint32
	const bigTime = uint32(1301986081)
	ts.Push(bigTime, 48.87, 2.27)
	ts.Push(bigTime+10, 48.87, 2.27)
	ts.Push(bigTime+20, 48.87, 2.27)

	itr := ts.Iter()
	i := 0
	for itr.Next() {
		i++
		if i == 5 {
			ts, lat, lng := itr.Values()
			if ts != aTime+41 {
				t.Fatal("got invalid final ts", ts, "expected", aTime+41)
			}
			if lat != 48.83004 {
				t.Fatal("got invalid final lat", lat, "expected", 48.83004)
			}
			if lng != 2.26 {
				t.Fatal("got invalid final lng", lng, "expected", 2.26)
			}
		}
		if i == 6 {
			ts, _, _ := itr.Values()
			if ts != bigTime {
				t.Fatal("got invalid final ts", ts, "expected", bigTime)
			}
		}
		if i == 8 {
			ts, lat, lng := itr.Values()
			if ts != bigTime+20 {
				t.Fatal("got invalid final ts", ts, "expected", bigTime+20)
			}
			if lat != 48.87 {
				t.Fatal("got invalid final lat", lat, "expected", 48.87)
			}
			if lng != 2.27 {
				t.Fatal("got invalid final lng", lng, "expected", 2.27)
			}
		}
	}
	if i != 8 {
		t.Fatal("expected 8 values got", i)
	}
}

func TestSize(t *testing.T) {
	const (
		aTime = uint32(1201986030)
		lat   = 39.93883
		lng   = 116.51135
	)
	ts := tsd.New()

	ts.Push(aTime, lat, lng) // 4 + 4 + 4 = 12
	b, _ := ts.MarshalBinary()
	if len(b) != 12 {
		t.Fatal("expected 12 values got", len(b))
	}
	ts.Push(aTime+10, lat, lng) // 1 + delta 1 = 2
	b, _ = ts.MarshalBinary()
	if len(b) != 14 {
		t.Fatal("expected 14 values got", len(b))
	}
	ts.Push(aTime+20, lat, lng) // 1
	b, _ = ts.MarshalBinary()
	if len(b) != 15 {
		t.Fatal("expected 15 values got", len(b))
	}
	ts.Push(aTime+30, lat+0.00001, lng) // 1 + delta lat 1 = 2
	b, _ = ts.MarshalBinary()
	if len(b) != 17 {
		t.Fatal("expected 17 values got", len(b))
	}
	ts.Push(aTime+40, lat+0.00001, lng) // 1 + delta lat 1 = 2
	b, _ = ts.MarshalBinary()
	if len(b) != 19 {
		t.Fatal("expected 19 values got", len(b))
	}
	ts.Push(aTime+50, lat+0.00001, lng) // 1
	b, _ = ts.MarshalBinary()
	if len(b) != 20 {
		t.Fatal("expected 20 values got", len(b))
	}
}

func TestEncodeDecode(t *testing.T) {
	for _, filename := range taxiDataFiles()[:10] {
		file, err := os.Open(filename)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("parsing ", filename)
		entries := readTSCoordAsEntries(file)
		file.Close()

		ts := tsd.New()
		for _, e := range entries {
			ts.Push(e.Ts, e.Lat, e.Lng)
		}
		itr := ts.Iter()
		i := 0
		for itr.Next() {
			ts, lat, lng := itr.Values()
			e := entries[i]
			if ts != e.Ts {
				t.Fatalf("%s ts not equal %d expected %d\nexpected: %s\nuncompressed: %d %f %f\nline %d",
					filename, ts, e.Ts, e.String(), ts, lng, lat, i+1)
			}
			if lat != e.Lat {
				t.Fatalf("Lat not equal %f expected %f file: %s line: %d", lat, e.Lat, filename, i+1)
			}
			if lng != e.Lng {
				t.Fatalf("Lng not equal %f expected %f file: %s line: %d", lng, e.Lng, filename, i+1)
			}
			// places   degrees          distance
			// -------  -------          --------
			// 5        0.00001          1.11 m

			i++
		}

		if len(entries) != i {
			t.Fatal("missing entries", i, len(entries))
		}
	}
}

func ExampleTimeSeries_Push() {
	ts := tsd.New()
	ts.Push(1201984833, 39.91071, 116.50962)
	ts.Push(1201985433, 39.91588, 116.52231)
	ts.Push(1201986033, 39.91445, 116.56444)
	b, _ := ts.MarshalBinary()
	fmt.Println(hex.EncodeToString(b))
	// Output: 47a4d541003ce61f00b1c7922a0258020504f528fd6c0b80
}

// readTSCoordAsFloats encodes time series as binary
// ts uint32, lng, lat float32
func readTSCoordAsFloats(r io.Reader) []byte {
	csvr := csv.NewReader(r)
	var ptsu uint32
	buf := new(bytes.Buffer)
	for {
		records, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		// parse date
		ts, err := time.Parse("2006-01-02 15:04:05", records[1])
		if err != nil {
			log.Fatal(err)
		}
		tsu := uint32(ts.Unix())
		// skip bogus entries
		if ptsu == tsu || ptsu > tsu {
			continue
		}
		ptsu = tsu

		err = binary.Write(buf, binary.BigEndian, tsu)
		if err != nil {
			log.Fatal(err)
		}

		// parse coordinates
		lng, err := strconv.ParseFloat(records[2], 32)
		if err != nil {
			log.Fatal(err)
		}
		lat, err := strconv.ParseFloat(records[3], 32)
		if err != nil {
			log.Fatal(err)
		}
		err = binary.Write(buf, binary.BigEndian, float32(lat))
		if err != nil {
			log.Fatal(err)
		}
		err = binary.Write(buf, binary.BigEndian, float32(lng))
		if err != nil {
			log.Fatal(err)
		}
	}
	return buf.Bytes()
}

// readTSCoordAsEntries reads TS as struct
func readTSCoordAsEntries(r io.Reader) []Entry {
	csvr := csv.NewReader(r)
	var ptsu uint32
	var res []Entry
	for {
		records, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		// parse date
		ts, err := time.Parse("2006-01-02 15:04:05", records[1])
		if err != nil {
			log.Fatal(err)
		}

		tsu := uint32(ts.Unix())
		// skip bogus entries
		if ptsu == tsu || ptsu > tsu {
			continue
		}
		ptsu = tsu

		// parse coordinates
		lng, err := strconv.ParseFloat(records[2], 32)
		if err != nil {
			log.Fatal(err)
		}
		lat, err := strconv.ParseFloat(records[3], 32)
		if err != nil {
			log.Fatal(err)
		}
		e := Entry{
			Ts:  tsu,
			Lat: float32(lat),
			Lng: float32(lng),
		}

		res = append(res, e)
	}
	return res
}

func taxiDataFiles() []string {
	files, err := ioutil.ReadDir("./testdata/taxi_log_2008_by_id")
	if err != nil {
		log.Fatal(err)
	}
	var res []string
	for _, file := range files {
		if file.Size() == 0 {
			continue
		}
		res = append(res, "./testdata/taxi_log_2008_by_id/"+file.Name())
	}
	return res
}

func (e *Entry) String() string {
	return fmt.Sprintf("%d %f %f", e.Ts, e.Lng, e.Lat)
}

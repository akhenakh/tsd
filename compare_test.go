package tsd

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
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
)

type Entry struct {
	Ts       uint32
	Lat, Lng float64
}

func TestCompareCompress(t *testing.T) {
	for _, filename := range taxiDataFiles()[:200] {
		file, err := os.Open(filename)
		if err != nil {
			t.Fatal(err)
		}
		b := readTSCoordAsFloats(file)

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

		// tsc
		file.Seek(0, 0)
		entries := readTSCoordAsEntries(file)
		file.Close()

		ts := New()
		for _, e := range entries {
			ts.Push(e.Ts, e.Lat, e.Lng)
		}

		b, _ = ts.MarshalBinary()
		tscSize := len(b)

		if lz4Size < tscSize {
			t.Log("LZ4 better Compressed", lz4Size, len(b))
		}
		if snapSize < tscSize {
			t.Log("Snappy better Compressed", snapSize, len(b))
		}

		t.Logf("Snappy %d LZ4 %d TSC %d", snapSize, lz4Size, tscSize)
	}
}

func TestEncodeDecode(t *testing.T) {
	for _, filename := range taxiDataFiles()[2:] {
		file, err := os.Open(filename)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Parsing", filename)
		entries := readTSCoordAsEntries(file)
		file.Close()

		ts := New()
		for _, e := range entries {
			ts.Push(e.Ts, e.Lat, e.Lng)
		}
		itr := ts.Iter()
		i := 0
		for itr.Next() {
			ts, lat, lng := itr.Values()
			e := entries[i]
			if ts != e.Ts {
				t.Fatalf("%s ts not equal %d expected %d\nexpected: %s\nuncompressed: %d %f %f",
					filename, ts, e.Ts, e.String(), ts, lat, lng)
			}
			// places   degrees          distance
			// -------  -------          --------
			// 5        0.00001          1.11 m
			if !inDelta(lat, e.Lat, 0.00002) {
				t.Fatal("Lat not in delta", lat, "expected", e.Lat)
			}
			if !inDelta(lng, e.Lng, 0.00002) {
				t.Fatal("Lng not in delta", lng, "expected", e.Lng)
			}
			i++
		}

		if len(entries) != i {
			t.Fatal("missing entries", i, len(entries))
		}

	}
}

// readTSCoordAsFloats encodes time series as binary
// ts uint32, lng, lat float64
func readTSCoordAsFloats(r io.Reader) []byte {
	csvr := csv.NewReader(r)

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
		err = binary.Write(buf, binary.BigEndian, tsu)
		if err != nil {
			log.Fatal(err)
		}
		// parse coordinates
		lng, err := strconv.ParseFloat(records[2], 64)
		if err != nil {
			log.Fatal(err)
		}
		lat, err := strconv.ParseFloat(records[3], 64)
		if err != nil {
			log.Fatal(err)
		}
		err = binary.Write(buf, binary.BigEndian, lat)
		if err != nil {
			log.Fatal(err)
		}
		err = binary.Write(buf, binary.BigEndian, lng)
		if err != nil {
			log.Fatal(err)
		}
	}
	return buf.Bytes()
}

// readTSCoordAsEntries reads TS as struct
func readTSCoordAsEntries(r io.Reader) []Entry {
	csvr := csv.NewReader(r)

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

		// parse coordinates
		lng, err := strconv.ParseFloat(records[2], 64)
		if err != nil {
			log.Fatal(err)
		}
		lat, err := strconv.ParseFloat(records[3], 64)
		if err != nil {
			log.Fatal(err)
		}
		e := Entry{
			Ts:  tsu,
			Lat: lat,
			Lng: lng,
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

func inDelta(v, expected, delta float64) bool {
	if v < expected-delta {
		return false
	}
	if v > expected+delta {
		return false
	}
	return true
}

func (e *Entry) String() string {
	return fmt.Sprintf("%d %f %f", e.Ts, e.Lng, e.Lat)
}

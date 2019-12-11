[![Build Status](https://cloud.drone.io/api/badges/akhenakh/tsd/status.svg)](https://cloud.drone.io/akhenakh/tsd)

TSD
---

TSD is a time series storage specialized in compressing increasing timestamp and geo spacial data (lat, lng).

It performs better than Snappy & LZ4, on this specific usage without any advanced optimizations but storing deltas and storage size reduction.

```
Size: 64776 Snappy 61007    LZ4 57225       TSD 23948
Size: 18864 Snappy 18870    LZ4 18267       TSD 9883
Size: 15768 Snappy 12721    LZ4 11860       TSD 4775
Size: 7764  Snappy 5440     LZ4 5148        TSD 2122
Size: 9168  Snappy 7251     LZ4 6933        TSD 3018
```

## Usage

It implements [`BinaryMarshaler`](https://golang.org/pkg/encoding/#BinaryMarshaler) and [`BinaryUnmarshaler`](https://golang.org/pkg/encoding/#BinaryUnmarshaler)
```go
ts := New()
ts.Push(1201984833, 39.91071, 116.50962)
ts.Push(1201985433, 39.91588, 116.52231)
ts.Push(1201986033, 39.91445, 116.56444)
b, _ := ts.MarshalBinary()
fmt.Println(hex.EncodeToString(b))
47a4d541003ce61f00b1c792290258020504f5290258ff711075
```

## Format is as follow

```
header ts uint32,   lat float32, lng float32
      32                  32           32

info,  time delta dyn,   lat delta dyn, lng delta dyn
 8            n,               n,             n
```

`n` can be 0, an int8, int16 or int32.

## Optimization 
The [Facebook's Gorilla paper](https://github.com/dgryski/go-tsz) would perform better but my needs were a bit different.

I needed a time window larger than the limit imposed by the paper (14 bits ~ 4 hours), and a way to store 2 values lat, lng that are closed to the previous one (moving devices).  
The timestamp storage is almost the same as the paper but for the values a delta of delta and appropriate storage per entry are good enough and perform better than dict compression.

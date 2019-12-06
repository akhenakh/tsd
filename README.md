TSD
---

TSD is a time series storage specialized in compressing increasing timestamp and geo spacial data (lat, lng).

It performs better than Snappy & LZ4, on this specific usage without any kind of optimizations but storing deltas.

```
Snappy 15338 LZ4 13583 TSC 7317
```

## Usage
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


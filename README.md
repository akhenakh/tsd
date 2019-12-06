TSD
---

TSD is a time series storage specialized in compressing increasing timestamp and geo spacial data (lat, lng).

It performs better than Snappy & LZ4, on this specific usage without any kind of optimizations but storing deltas.

```
Snappy 15338 LZ4 13583 TSC 7317
```

Format is as follow

```
header ts uint32,   lat float32, lng float32
      32                  32           32

info,  time delta dyn,   lat delta dyn, lng delta dyn
 8            n,               n,             n
```

`n` can be 0, an int8, int16 or int32.


package tsd

type DeltaEncoding uint8

const (
	Delta0  DeltaEncoding = 0b00
	Delta8  DeltaEncoding = 0b01
	Delta16 DeltaEncoding = 0b10
	Full32  DeltaEncoding = 0b11
)

// TSDelta returns delta encoding for the timestamp
func (d DeltaEncoding) TSDelta() DeltaEncoding {
	return d & 0b000011
}

// LatDelta returns delta encoding for latitude
func (d DeltaEncoding) LatDelta() DeltaEncoding {
	return d & 0b001100 >> 2
}

// LngDelta returns delta encoding for longitude
func (d DeltaEncoding) LngDelta() DeltaEncoding {
	return d & 0b110000 >> 4
}

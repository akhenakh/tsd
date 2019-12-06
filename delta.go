package tsd

type DeltaEncoding uint8

const (
	TSDelta8  DeltaEncoding = 0b00
	TSDelta16               = 0b01
	TSFull32                = 0b10

	LatDelta0  = 0b00
	LatDelta8  = 0b01
	LatDelta16 = 0b10
	LatFull32  = 0b11

	LngDelta0  = 0b00
	LngDelta8  = 0b01
	LngDelta16 = 0b10
	LngFull32  = 0b11
)

func (d DeltaEncoding) TSDelta() DeltaEncoding {
	return d & 0b000011
}

func (d DeltaEncoding) LatDelta() DeltaEncoding {
	return d & 0b001100 >> 2
}
func (d DeltaEncoding) LngDelta() DeltaEncoding {
	return d & 0b110000 >> 4
}

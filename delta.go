package tsd

type DeltaEncoding uint8

const (
	TSDelta0  DeltaEncoding = 0b00
	TSDelta8  DeltaEncoding = 0b01
	TSDelta16 DeltaEncoding = 0b10
	TSFull32  DeltaEncoding = 0b11

	LatDelta0  DeltaEncoding = 0b00
	LatDelta8  DeltaEncoding = 0b01
	LatDelta16 DeltaEncoding = 0b10
	LatFull32  DeltaEncoding = 0b11

	LngDelta0  DeltaEncoding = 0b00
	LngDelta8  DeltaEncoding = 0b01
	LngDelta16 DeltaEncoding = 0b10
	LngFull32  DeltaEncoding = 0b11
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

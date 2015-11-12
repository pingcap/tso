package proto

type RequestHeader struct {
}

type Timestamp struct {
	Physical int64
	Logical  int64
}

type Response struct {
	Timestamp
}

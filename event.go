package gotelemetrypipeline

type Event struct {
	Timestamp *uint64  `json:"timestamp"`
	Key       string   `json:"key"`
	Value     *float64 `json:"value"`
}

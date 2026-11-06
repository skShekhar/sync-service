package scheduler

import (
	"sync"
	"time"
)

type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func NewTokenBucket(rate, burst float64, now time.Time) *tokenBucket {
	return &tokenBucket{
		rate:   rate,
		burst:  burst,
		tokens: burst,
		last:   now,
	}
}

func (tb *tokenBucket) Allow(now time.Time) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	timeElapsed := now.Sub(tb.last).Seconds()
	tb.tokens = min(tb.burst, tb.tokens+timeElapsed*tb.rate)
	tb.last = now
	if tb.tokens < 1 {
		return false
	}
	tb.tokens--
	return true
}

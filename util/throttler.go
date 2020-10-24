package util

import (
	"time"
)

func Thorttle(throttle time.Duration, nextAllowed time.Time) time.Time {
	now := time.Now()
	if nextAllowed.IsZero() {
		nextAllowed = now
	}
	diff := nextAllowed.Sub(now)
	if diff > 0 {
		time.Sleep(diff)
	}
	return nextAllowed.Add(throttle)
}

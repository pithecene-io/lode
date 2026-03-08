package lode

import (
	"context"
	"testing"
	"time"
)

func TestJitteredBackoff_Bounds(t *testing.T) {
	cfg := retryConfig{
		baseDelay: 10 * time.Millisecond,
		maxDelay:  2 * time.Second,
		jitter:    1.0,
	}

	for attempt := 1; attempt <= 10; attempt++ {
		delay := cfg.jitteredBackoff(attempt)
		if delay < 0 {
			t.Errorf("attempt %d: negative delay %v", attempt, delay)
		}
		if delay > cfg.maxDelay {
			t.Errorf("attempt %d: delay %v exceeds max %v", attempt, delay, cfg.maxDelay)
		}
	}
}

func TestJitteredBackoff_NoJitter_Deterministic(t *testing.T) {
	cfg := retryConfig{
		baseDelay: 10 * time.Millisecond,
		maxDelay:  2 * time.Second,
		jitter:    0.0,
	}

	// With zero jitter, delay should be exactly baseDelay * 2^(attempt-1).
	expected := []time.Duration{
		10 * time.Millisecond,  // attempt 1
		20 * time.Millisecond,  // attempt 2
		40 * time.Millisecond,  // attempt 3
		80 * time.Millisecond,  // attempt 4
		160 * time.Millisecond, // attempt 5
	}
	for i, want := range expected {
		got := cfg.jitteredBackoff(i + 1)
		if got != want {
			t.Errorf("attempt %d: got %v, want %v", i+1, got, want)
		}
	}
}

func TestJitteredBackoff_CappedAtMaxDelay(t *testing.T) {
	cfg := retryConfig{
		baseDelay: 100 * time.Millisecond,
		maxDelay:  500 * time.Millisecond,
		jitter:    0.0,
	}

	// attempt 1: 100ms, attempt 2: 200ms, attempt 3: 400ms, attempt 4: capped at 500ms
	got := cfg.jitteredBackoff(4)
	if got != 500*time.Millisecond {
		t.Errorf("attempt 4: got %v, want 500ms (capped)", got)
	}
}

func TestRetryBackoff_ContextCanceled(t *testing.T) {
	cfg := retryConfig{
		baseDelay: 10 * time.Second, // long delay so context cancels first
		maxDelay:  10 * time.Second,
		jitter:    0.0,
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := cfg.retryBackoff(ctx, 1)
	if err == nil {
		t.Fatal("expected context canceled error")
	}
}

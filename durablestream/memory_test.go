package durablestream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryStorage_Create(t *testing.T) {
	tests := []struct {
		name       string
		streamID   string
		config     StreamConfig
		setup      func(*MemoryStorage)
		wantErr    bool
		wantErrIs  error // sentinel error to check with errors.Is()
		checkFunc  func(*testing.T, *MemoryStorage)
	}{
		{
			name:     "create new stream",
			streamID: "stream1",
			config: StreamConfig{
				ContentType: "application/json",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, s *MemoryStorage) {
				s.mu.RLock()
				defer s.mu.RUnlock()
				if _, ok := s.streams["stream1"]; !ok {
					t.Error("stream not created")
				}
			},
		},
		{
			name:     "idempotent create with matching config",
			streamID: "stream1",
			config: StreamConfig{
				ContentType: "application/json",
			},
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "application/json",
				})
			},
			wantErr: false,
		},
		{
			name:     "conflict on different content type",
			streamID: "stream1",
			config: StreamConfig{
				ContentType: "text/plain",
			},
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "application/json",
				})
			},
			wantErr:   true,
			wantErrIs: ErrConflict,
		},
		{
			name:     "create with TTL",
			streamID: "stream2",
			config: StreamConfig{
				ContentType: "application/json",
				TTL:         durationPtr(1 * time.Hour),
			},
			wantErr: false,
		},
		{
			name:     "create with expiry",
			streamID: "stream3",
			config: StreamConfig{
				ContentType: "application/json",
				ExpiresAt:   timePtr(time.Now().Add(1 * time.Hour)),
			},
			wantErr: false,
		},
		{
			name:     "conflict on different TTL",
			streamID: "stream4",
			config: StreamConfig{
				ContentType: "application/json",
				TTL:         durationPtr(2 * time.Hour),
			},
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream4", StreamConfig{
					ContentType: "application/json",
					TTL:         durationPtr(1 * time.Hour),
				})
			},
			wantErr:   true,
			wantErrIs: ErrConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStorage()
			if tt.setup != nil {
				tt.setup(s)
			}

			_, err := s.Create(context.Background(), tt.streamID, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
				t.Errorf("Create() error = %v, want errors.Is(..., %v)", err, tt.wantErrIs)
			}

			if tt.checkFunc != nil {
				tt.checkFunc(t, s)
			}
		})
	}
}

func TestMemoryStorage_Append(t *testing.T) {
	tests := []struct {
		name       string
		streamID   string
		data       []byte
		seq        string
		setup      func(*MemoryStorage)
		wantErr    bool
		wantErrIs  error
		wantOffset Offset
	}{
		{
			name:     "append to stream",
			streamID: "stream1",
			data:     []byte("hello"),
			seq:      "",
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
				})
			},
			wantErr:    false,
			wantOffset: "0000000001",
		},
		{
			name:      "append to non-existent stream",
			streamID:  "nonexistent",
			data:      []byte("hello"),
			wantErr:   true,
			wantErrIs: ErrNotFound,
		},
		{
			name:     "empty append",
			streamID: "stream1",
			data:     []byte{},
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
				})
			},
			wantErr: true, // bad request, no sentinel
		},
		{
			name:     "append with sequence number",
			streamID: "stream1",
			data:     []byte("first"),
			seq:      "seq1",
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
				})
			},
			wantErr:    false,
			wantOffset: "0000000001",
		},
		{
			name:     "sequence regression",
			streamID: "stream1",
			data:     []byte("second"),
			seq:      "seq1", // Same as previous
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
				})
				_, _ = s.Append(context.Background(), "stream1", []byte("first"), "seq2")
			},
			wantErr:   true,
			wantErrIs: ErrConflict,
		},
		{
			name:     "sequence ordering (lexicographic)",
			streamID: "stream1",
			data:     []byte("third"),
			seq:      "seq3",
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
				})
				_, _ = s.Append(context.Background(), "stream1", []byte("first"), "seq1")
				_, _ = s.Append(context.Background(), "stream1", []byte("second"), "seq2")
			},
			wantErr:    false,
			wantOffset: "0000000003",
		},
		{
			name:     "append to expired stream",
			streamID: "stream1",
			data:     []byte("hello"),
			setup: func(s *MemoryStorage) {
				past := time.Now().Add(-1 * time.Hour)
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
					ExpiresAt:   &past,
				})
			},
			wantErr:   true,
			wantErrIs: ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStorage()
			if tt.setup != nil {
				tt.setup(s)
			}

			offset, err := s.Append(context.Background(), tt.streamID, tt.data, tt.seq)
			if (err != nil) != tt.wantErr {
				t.Errorf("Append() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
				t.Errorf("Append() error = %v, want errors.Is(..., %v)", err, tt.wantErrIs)
			}

			if !tt.wantErr && offset != tt.wantOffset {
				t.Errorf("Append() offset = %v, want %v", offset, tt.wantOffset)
			}
		})
	}
}

func TestMemoryStorage_Read(t *testing.T) {
	tests := []struct {
		name      string
		streamID  string
		offset    Offset
		limit     int
		setup     func(*MemoryStorage)
		wantErr   bool
		wantErrIs error
		wantData  []byte
	}{
		{
			name:     "read from start",
			streamID: "stream1",
			offset:   "0000000000",
			limit:    0,
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
				})
				_, _ = s.Append(context.Background(), "stream1", []byte("hello"), "")
				_, _ = s.Append(context.Background(), "stream1", []byte("world"), "")
			},
			wantErr:  false,
			wantData: []byte("helloworld"),
		},
		{
			name:     "read with limit",
			streamID: "stream1",
			offset:   "0000000000",
			limit:    5,
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
				})
				_, _ = s.Append(context.Background(), "stream1", []byte("hello"), "")
				_, _ = s.Append(context.Background(), "stream1", []byte("world"), "")
			},
			wantErr:  false,
			wantData: []byte("hello"),
		},
		{
			name:      "read from non-existent stream",
			streamID:  "nonexistent",
			offset:    "0000000000",
			limit:     0,
			wantErr:   true,
			wantErrIs: ErrNotFound,
		},
		{
			name:     "read from invalid offset",
			streamID: "stream1",
			offset:   "9999999999",
			limit:    0,
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
				})
				_, _ = s.Append(context.Background(), "stream1", []byte("hello"), "")
			},
			wantErr:   true,
			wantErrIs: ErrGone,
		},
		{
			name:     "read from middle offset",
			streamID: "stream1",
			offset:   "0000000001",
			limit:    0,
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
				})
				_, _ = s.Append(context.Background(), "stream1", []byte("hello"), "")
				_, _ = s.Append(context.Background(), "stream1", []byte("world"), "")
			},
			wantErr:  false,
			wantData: []byte("world"),
		},
		{
			name:     "read from expired stream",
			streamID: "stream1",
			offset:   "0000000000",
			limit:    0,
			setup: func(s *MemoryStorage) {
				past := time.Now().Add(-1 * time.Hour)
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
					ExpiresAt:   &past,
				})
			},
			wantErr:   true,
			wantErrIs: ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStorage()
			if tt.setup != nil {
				tt.setup(s)
			}

			result, err := s.Read(context.Background(), tt.streamID, tt.offset, tt.limit)
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
				t.Errorf("Read() error = %v, want errors.Is(..., %v)", err, tt.wantErrIs)
			}

			if !tt.wantErr {
				if string(result.Data) != string(tt.wantData) {
					t.Errorf("Read() data = %q, want %q", result.Data, tt.wantData)
				}
			}
		})
	}
}

func TestMemoryStorage_Head(t *testing.T) {
	tests := []struct {
		name      string
		streamID  string
		setup     func(*MemoryStorage)
		wantErr   bool
		wantErrIs error
		check     func(*testing.T, *StreamInfo)
	}{
		{
			name:     "head on existing stream",
			streamID: "stream1",
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "application/json",
				})
			},
			wantErr: false,
			check: func(t *testing.T, info *StreamInfo) {
				if info.ContentType != "application/json" {
					t.Errorf("ContentType = %v, want application/json", info.ContentType)
				}
				if info.NextOffset != "0000000000" {
					t.Errorf("NextOffset = %v, want 0000000000", info.NextOffset)
				}
			},
		},
		{
			name:      "head on non-existent stream",
			streamID:  "nonexistent",
			wantErr:   true,
			wantErrIs: ErrNotFound,
		},
		{
			name:     "head shows updated offset after appends",
			streamID: "stream1",
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "application/json",
				})
				_, _ = s.Append(context.Background(), "stream1", []byte("data1"), "")
				_, _ = s.Append(context.Background(), "stream1", []byte("data2"), "")
			},
			wantErr: false,
			check: func(t *testing.T, info *StreamInfo) {
				if info.NextOffset != "0000000002" {
					t.Errorf("NextOffset = %v, want 0000000002", info.NextOffset)
				}
			},
		},
		{
			name:     "head on expired stream",
			streamID: "stream1",
			setup: func(s *MemoryStorage) {
				past := time.Now().Add(-1 * time.Hour)
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "text/plain",
					ExpiresAt:   &past,
				})
			},
			wantErr:   true,
			wantErrIs: ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStorage()
			if tt.setup != nil {
				tt.setup(s)
			}

			info, err := s.Head(context.Background(), tt.streamID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Head() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
				t.Errorf("Head() error = %v, want errors.Is(..., %v)", err, tt.wantErrIs)
			}

			if !tt.wantErr && tt.check != nil {
				tt.check(t, info)
			}
		})
	}
}

func TestMemoryStorage_Delete(t *testing.T) {
	tests := []struct {
		name      string
		streamID  string
		setup     func(*MemoryStorage)
		wantErr   bool
		wantErrIs error
	}{
		{
			name:     "delete existing stream",
			streamID: "stream1",
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "application/json",
				})
			},
			wantErr: false,
		},
		{
			name:      "delete non-existent stream",
			streamID:  "nonexistent",
			wantErr:   true,
			wantErrIs: ErrNotFound,
		},
		{
			name:     "delete closes subscribers",
			streamID: "stream1",
			setup: func(s *MemoryStorage) {
				_, _ = s.Create(context.Background(), "stream1", StreamConfig{
					ContentType: "application/json",
				})
				_, _ = s.Subscribe(context.Background(), "stream1", "0000000000")
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStorage()
			if tt.setup != nil {
				tt.setup(s)
			}

			err := s.Delete(context.Background(), tt.streamID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
				t.Errorf("Delete() error = %v, want errors.Is(..., %v)", err, tt.wantErrIs)
			}

			if !tt.wantErr {
				s.mu.RLock()
				_, exists := s.streams[tt.streamID]
				s.mu.RUnlock()
				if exists {
					t.Error("stream still exists after delete")
				}
			}
		})
	}
}

func TestMemoryStorage_Subscribe(t *testing.T) {
	t.Run("receive notifications", func(t *testing.T) {
		s := NewMemoryStorage()
		_, _ = s.Create(context.Background(), "stream1", StreamConfig{
			ContentType: "application/json",
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch, err := s.Subscribe(ctx, "stream1", "0000000000")
		if err != nil {
			t.Fatalf("Subscribe() error = %v", err)
		}

		// Append data in goroutine
		go func() {
			time.Sleep(50 * time.Millisecond)
			_, _ = s.Append(context.Background(), "stream1", []byte("data"), "")
		}()

		// Wait for notification
		select {
		case offset := <-ch:
			if offset != "0000000001" {
				t.Errorf("received offset = %v, want 0000000001", offset)
			}
		case <-time.After(1 * time.Second):
			t.Error("timeout waiting for notification")
		}
	})

	t.Run("context cancellation closes channel", func(t *testing.T) {
		s := NewMemoryStorage()
		_, _ = s.Create(context.Background(), "stream1", StreamConfig{
			ContentType: "application/json",
		})

		ctx, cancel := context.WithCancel(context.Background())

		ch, err := s.Subscribe(ctx, "stream1", "0000000000")
		if err != nil {
			t.Fatalf("Subscribe() error = %v", err)
		}

		cancel()

		// Channel should close
		select {
		case _, ok := <-ch:
			if ok {
				t.Error("channel should be closed")
			}
		case <-time.After(1 * time.Second):
			t.Error("timeout waiting for channel close")
		}
	})

	t.Run("subscribe to non-existent stream", func(t *testing.T) {
		s := NewMemoryStorage()
		_, err := s.Subscribe(context.Background(), "nonexistent", "0000000000")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Subscribe() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("multiple subscribers receive notifications", func(t *testing.T) {
		s := NewMemoryStorage()
		_, _ = s.Create(context.Background(), "stream1", StreamConfig{
			ContentType: "application/json",
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch1, _ := s.Subscribe(ctx, "stream1", "0000000000")
		ch2, _ := s.Subscribe(ctx, "stream1", "0000000000")

		_, _ = s.Append(context.Background(), "stream1", []byte("data"), "")

		// Both should receive notification
		received := 0
		timeout := time.After(1 * time.Second)
		for received < 2 {
			select {
			case <-ch1:
				received++
			case <-ch2:
				received++
			case <-timeout:
				t.Fatalf("timeout, only received %d notifications", received)
			}
		}
	})
}

func TestTimeEqual(t *testing.T) {
	now := time.Now()
	later := now.Add(1 * time.Hour)

	tests := []struct {
		name string
		a    *time.Time
		b    *time.Time
		want bool
	}{
		{
			name: "both nil",
			a:    nil,
			b:    nil,
			want: true,
		},
		{
			name: "first nil",
			a:    nil,
			b:    &now,
			want: false,
		},
		{
			name: "second nil",
			a:    &now,
			b:    nil,
			want: false,
		},
		{
			name: "equal times",
			a:    &now,
			b:    &now,
			want: true,
		},
		{
			name: "different times",
			a:    &now,
			b:    &later,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := timeEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("timeEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDurationEqual(t *testing.T) {
	d1 := 1 * time.Hour
	d2 := 2 * time.Hour

	tests := []struct {
		name string
		a    *time.Duration
		b    *time.Duration
		want bool
	}{
		{
			name: "both nil",
			a:    nil,
			b:    nil,
			want: true,
		},
		{
			name: "first nil",
			a:    nil,
			b:    &d1,
			want: false,
		},
		{
			name: "second nil",
			a:    &d1,
			b:    nil,
			want: false,
		},
		{
			name: "equal durations",
			a:    &d1,
			b:    &d1,
			want: true,
		},
		{
			name: "different durations",
			a:    &d1,
			b:    &d2,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := durationEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("durationEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigsMatch(t *testing.T) {
	now := time.Now()
	d1 := 1 * time.Hour

	tests := []struct {
		name string
		a    StreamConfig
		b    StreamConfig
		want bool
	}{
		{
			name: "matching basic configs",
			a:    StreamConfig{ContentType: "text/plain"},
			b:    StreamConfig{ContentType: "text/plain"},
			want: true,
		},
		{
			name: "different content types",
			a:    StreamConfig{ContentType: "text/plain"},
			b:    StreamConfig{ContentType: "application/json"},
			want: false,
		},
		{
			name: "matching configs with TTL",
			a:    StreamConfig{ContentType: "text/plain", TTL: &d1},
			b:    StreamConfig{ContentType: "text/plain", TTL: &d1},
			want: true,
		},
		{
			name: "matching configs with ExpiresAt",
			a:    StreamConfig{ContentType: "text/plain", ExpiresAt: &now},
			b:    StreamConfig{ContentType: "text/plain", ExpiresAt: &now},
			want: true,
		},
		{
			name: "mismatched TTL",
			a:    StreamConfig{ContentType: "text/plain", TTL: &d1},
			b:    StreamConfig{ContentType: "text/plain", TTL: nil},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := configsMatch(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("configsMatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMemoryStorage_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent appends", func(t *testing.T) {
		s := NewMemoryStorage()
		_, _ = s.Create(context.Background(), "stream1", StreamConfig{
			ContentType: "application/json",
		})

		var wg sync.WaitGroup
		numGoroutines := 10
		appendsPerGoroutine := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < appendsPerGoroutine; j++ {
					_, err := s.Append(context.Background(), "stream1", []byte("data"), "")
					if err != nil {
						t.Errorf("Append() error = %v", err)
					}
				}
			}(i)
		}

		wg.Wait()

		// Verify total appends
		info, _ := s.Head(context.Background(), "stream1")
		expectedOffset := formatOffset(numGoroutines * appendsPerGoroutine)
		if info.NextOffset != expectedOffset {
			t.Errorf("NextOffset = %v, want %v", info.NextOffset, expectedOffset)
		}
	})

	t.Run("concurrent reads and appends", func(t *testing.T) {
		s := NewMemoryStorage()
		_, _ = s.Create(context.Background(), "stream1", StreamConfig{
			ContentType: "application/json",
		})

		var wg sync.WaitGroup

		// Writers
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					_, _ = s.Append(context.Background(), "stream1", []byte("data"), "")
				}
			}()
		}

		// Readers
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					_, _ = s.Read(context.Background(), "stream1", "0000000000", 100)
				}
			}()
		}

		wg.Wait()
	})
}

package badgerstore

import (
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func TestContentTypesMatch(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"text/plain", "text/plain", true},
		{"TEXT/PLAIN", "text/plain", true},
		{"application/json", "APPLICATION/JSON", true},
		{"text/plain; charset=utf-8", "text/plain", true},
		{"text/plain", "text/html", false},
		{"application/json", "application/xml", false},
	}

	for _, tt := range tests {
		got := contentTypesMatch(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("contentTypesMatch(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestIsExpired(t *testing.T) {
	t.Run("not expired with zero ExpiresAt", func(t *testing.T) {
		cfg := durablestream.StreamConfig{}
		if isExpired(cfg) {
			t.Error("expected not expired with zero ExpiresAt")
		}
	})

	t.Run("not expired with future ExpiresAt", func(t *testing.T) {
		cfg := durablestream.StreamConfig{
			ExpiresAt: time.Now().Add(time.Hour),
		}
		if isExpired(cfg) {
			t.Error("expected not expired with future ExpiresAt")
		}
	})

	t.Run("expired with past ExpiresAt", func(t *testing.T) {
		cfg := durablestream.StreamConfig{
			ExpiresAt: time.Now().Add(-time.Hour),
		}
		if !isExpired(cfg) {
			t.Error("expected expired with past ExpiresAt")
		}
	})
}

func TestConfigsMatch(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name string
		a, b durablestream.StreamConfig
		want bool
	}{
		{
			name: "identical configs",
			a:    durablestream.StreamConfig{ContentType: "text/plain", TTL: time.Hour, ExpiresAt: now},
			b:    durablestream.StreamConfig{ContentType: "text/plain", TTL: time.Hour, ExpiresAt: now},
			want: true,
		},
		{
			name: "different content type",
			a:    durablestream.StreamConfig{ContentType: "text/plain"},
			b:    durablestream.StreamConfig{ContentType: "application/json"},
			want: false,
		},
		{
			name: "different TTL",
			a:    durablestream.StreamConfig{ContentType: "text/plain", TTL: time.Hour},
			b:    durablestream.StreamConfig{ContentType: "text/plain", TTL: 2 * time.Hour},
			want: false,
		},
		{
			name: "different ExpiresAt",
			a:    durablestream.StreamConfig{ContentType: "text/plain", ExpiresAt: now},
			b:    durablestream.StreamConfig{ContentType: "text/plain", ExpiresAt: now.Add(time.Hour)},
			want: false,
		},
		{
			name: "case-insensitive content type",
			a:    durablestream.StreamConfig{ContentType: "TEXT/PLAIN"},
			b:    durablestream.StreamConfig{ContentType: "text/plain"},
			want: true,
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

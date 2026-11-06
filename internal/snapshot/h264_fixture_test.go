//go:build cgo && openh264

package snapshot

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDecodeMainProfileIDRFixture(t *testing.T) {
	decodeFixture(t, "main_idr.h264")
}

func decodeFixture(t *testing.T, name string) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	path := filepath.Join(filepath.Dir(thisFile), "testdata", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	frame, err := DecodeIDRFrame(data)
	if err != nil {
		t.Fatalf("DecodeIDRFrame(%s): %v", name, err)
	}
	if frame.Bounds().Dx() <= 0 || frame.Bounds().Dy() <= 0 {
		t.Fatalf("invalid dimensions: %v", frame.Bounds())
	}
}

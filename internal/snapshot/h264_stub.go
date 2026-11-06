//go:build !cgo || !openh264

package snapshot

import (
	"errors"
	"image"
)

var ErrCGORequired = errors.New("snapshot decode requires cgo+openh264 build")

func DecoderAvailable() bool {
	return false
}

func DecodeIDRFrame(_ []byte) (*image.YCbCr, error) {
	return nil, ErrCGORequired
}

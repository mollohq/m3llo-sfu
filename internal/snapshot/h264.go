//go:build cgo && openh264

package snapshot

/*
#cgo LDFLAGS: -lopenh264
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <wels/codec_api.h>

typedef struct decoded_frame_t {
	uint8_t* y;
	uint8_t* cb;
	uint8_t* cr;
	int y_len;
	int c_len;
	int width;
	int height;
	int y_stride;
	int c_stride;
} decoded_frame_t;

static int set_error(char** err, const char* msg) {
	if (err != NULL) {
		*err = strdup(msg);
	}
	return -1;
}

static int find_start_code(const uint8_t* data, int len, int from, int* sc_len) {
	for (int i = from; i + 3 < len; i++) {
		if (data[i] == 0 && data[i + 1] == 0) {
			if (data[i + 2] == 1) {
				*sc_len = 3;
				return i;
			}
			if (i + 3 < len && data[i + 2] == 0 && data[i + 3] == 1) {
				*sc_len = 4;
				return i;
			}
		}
	}
	return -1;
}

static int decode_chunk(ISVCDecoder* decoder, const uint8_t* data, int data_len,
		unsigned char** dst, SBufferInfo* dstInfo) {
	DECODING_STATE state = (*decoder)->DecodeFrameNoDelay(decoder, data, data_len, dst, dstInfo);
	if (state != dsErrorFree && state != dsFramePending) {
		return -1;
	}
	return dstInfo->iBufferStatus == 1 ? 1 : 0;
}

static int feed_annex_b_nals(ISVCDecoder* decoder, const uint8_t* data, int data_len,
		unsigned char** dst, SBufferInfo* dstInfo) {
	int offset = 0;
	while (offset < data_len) {
		int sc_len = 0;
		int sc = find_start_code(data, data_len, offset, &sc_len);
		if (sc < 0) {
			break;
		}
		int nal_start = sc;
		int next_sc_len = 0;
		int next = find_start_code(data, data_len, sc + sc_len, &next_sc_len);
		int nal_end = next >= 0 ? next : data_len;
		int chunk_len = nal_end - nal_start;
		if (chunk_len > 0) {
			int got = decode_chunk(decoder, data + nal_start, chunk_len, dst, dstInfo);
			if (got < 0) {
				return -1;
			}
			if (got > 0) {
				return 1;
			}
		}
		if (next < 0) {
			break;
		}
		offset = next;
	}
	return 0;
}

static int flush_decoder(ISVCDecoder* decoder, unsigned char** dst, SBufferInfo* dstInfo) {
	for (int flush = 0; flush < 16; flush++) {
		int got = decode_chunk(decoder, NULL, 0, dst, dstInfo);
		if (got < 0) {
			return -1;
		}
		if (got > 0) {
			return 1;
		}
	}
	return 0;
}

static int feed_annex_b_stream(ISVCDecoder* decoder, const uint8_t* data, int data_len,
		unsigned char** dst, SBufferInfo* dstInfo) {
	// Prime with the full access unit. Baseline streams may decode here; Main
	// profile streams need the per-NAL pass below after priming.
	int got = decode_chunk(decoder, data, data_len, dst, dstInfo);
	if (got < 0) {
		return -1;
	}
	if (got > 0) {
		return 1;
	}

	got = feed_annex_b_nals(decoder, data, data_len, dst, dstInfo);
	if (got < 0) {
		return -1;
	}
	if (got > 0) {
		return 1;
	}

	return flush_decoder(decoder, dst, dstInfo);
}

int decode_idr_frame(const uint8_t* data, int data_len, decoded_frame_t* out, char** err) {
	if (data == NULL || data_len <= 0 || out == NULL) {
		return set_error(err, "invalid decoder input");
	}

	ISVCDecoder* decoder = NULL;
	if (WelsCreateDecoder(&decoder) != 0 || decoder == NULL) {
		return set_error(err, "WelsCreateDecoder failed");
	}

	SDecodingParam params;
	memset(&params, 0, sizeof(params));
	params.uiTargetDqLayer = (unsigned char)-1;
	params.eEcActiveIdc = ERROR_CON_SLICE_COPY;
	params.sVideoProperty.eVideoBsType = VIDEO_BITSTREAM_DEFAULT;
	params.sVideoProperty.size = sizeof(params.sVideoProperty);

	if ((*decoder)->Initialize(decoder, &params) != cmResultSuccess) {
		(*decoder)->Uninitialize(decoder);
		WelsDestroyDecoder(decoder);
		return set_error(err, "decoder initialization failed");
	}

	unsigned char* dst[3] = {NULL, NULL, NULL};
	SBufferInfo dstInfo;
	memset(&dstInfo, 0, sizeof(dstInfo));

	int got = feed_annex_b_stream(decoder, data, data_len, dst, &dstInfo);
	if (got <= 0) {
		(*decoder)->Uninitialize(decoder);
		WelsDestroyDecoder(decoder);
		if (got < 0) {
			return set_error(err, "openh264 decode failed");
		}
		return set_error(err, "decoder did not emit a frame");
	}

	const int width = dstInfo.UsrData.sSystemBuffer.iWidth;
	const int height = dstInfo.UsrData.sSystemBuffer.iHeight;
	const int yStride = dstInfo.UsrData.sSystemBuffer.iStride[0];
	const int cStride = dstInfo.UsrData.sSystemBuffer.iStride[1];
	const int cHeight = (height + 1) / 2;
	const int yLen = yStride * height;
	const int cLen = cStride * cHeight;

	out->y = (uint8_t*)malloc((size_t)yLen);
	out->cb = (uint8_t*)malloc((size_t)cLen);
	out->cr = (uint8_t*)malloc((size_t)cLen);
	if (out->y == NULL || out->cb == NULL || out->cr == NULL) {
		free(out->y);
		free(out->cb);
		free(out->cr);
		(*decoder)->Uninitialize(decoder);
		WelsDestroyDecoder(decoder);
		return set_error(err, "malloc failed for decoded frame buffers");
	}

	memcpy(out->y, dst[0], (size_t)yLen);
	memcpy(out->cb, dst[1], (size_t)cLen);
	memcpy(out->cr, dst[2], (size_t)cLen);
	out->y_len = yLen;
	out->c_len = cLen;
	out->width = width;
	out->height = height;
	out->y_stride = yStride;
	out->c_stride = cStride;

	(*decoder)->Uninitialize(decoder);
	WelsDestroyDecoder(decoder);
	return 0;
}

void free_decoded_frame(decoded_frame_t* out) {
	if (out == NULL) {
		return;
	}
	free(out->y);
	free(out->cb);
	free(out->cr);
	out->y = NULL;
	out->cb = NULL;
	out->cr = NULL;
}

void free_error(char* err) {
	free(err);
}
*/
import "C"

import (
	"errors"
	"image"
	"unsafe"
)

func DecoderAvailable() bool {
	return true
}

func DecodeIDRFrame(data []byte) (*image.YCbCr, error) {
	if len(data) == 0 {
		return nil, errors.New("empty H264 payload")
	}

	var frame C.decoded_frame_t
	var cErr *C.char

	result := C.decode_idr_frame(
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.int(len(data)),
		&frame,
		&cErr,
	)
	if result != 0 {
		if cErr != nil {
			defer C.free_error(cErr)
			return nil, errors.New(C.GoString(cErr))
		}
		return nil, errors.New("openh264 decode failed")
	}
	defer C.free_decoded_frame(&frame)

	width := int(frame.width)
	height := int(frame.height)
	if width <= 0 || height <= 0 {
		return nil, errors.New("invalid decoded dimensions")
	}

	y := C.GoBytes(unsafe.Pointer(frame.y), C.int(frame.y_len))
	cb := C.GoBytes(unsafe.Pointer(frame.cb), C.int(frame.c_len))
	cr := C.GoBytes(unsafe.Pointer(frame.cr), C.int(frame.c_len))

	return &image.YCbCr{
		Y:              y,
		Cb:             cb,
		Cr:             cr,
		YStride:        int(frame.y_stride),
		CStride:        int(frame.c_stride),
		SubsampleRatio: image.YCbCrSubsampleRatio420,
		Rect:           image.Rect(0, 0, width, height),
	}, nil
}

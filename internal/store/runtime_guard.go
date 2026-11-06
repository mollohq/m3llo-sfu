package store

import "strings"

// unsafeRuntime reports a Go runtime with golang/go#77975: Go 1.26.0 and
// 1.26.1 on Windows replace the high 32 bits of a pointer in overlapped
// socket I/O with 16 or 28 (the IPv4 or IPv6 sockaddr length), and the
// process faults under media traffic. The bug is in the runtime, not in
// the store: the SFU faults without the store too, the store only changes
// the timing. Go 1.26.2 has the fix. On such a runtime the store stays
// off, so a crash cannot take the history with it mid-write.
func unsafeRuntime(goos, version string) bool {
	if goos != "windows" {
		return false
	}
	v := strings.TrimPrefix(strings.Fields(version + " ")[0], "go")
	switch {
	case v == "1.26", v == "1.26.0", v == "1.26.1":
		return true
	case strings.HasPrefix(v, "1.26rc"):
		return true
	}
	return false
}

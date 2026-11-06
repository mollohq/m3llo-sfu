//go:build windows

package metrics

import (
	"syscall"
	"unsafe"
)

var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	procGlobalMemoryStatusEx = kernel32.NewProc("GlobalMemoryStatusEx")
)

// memoryStatusEx mirrors the Win32 MEMORYSTATUSEX structure.
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32 // percent of physical memory in use (0-100)
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

// hostMemoryPercent returns used physical memory as a percent of total (0-100)
// via GlobalMemoryStatusEx. This path exists mainly so local dev on Windows
// reports a real number; production runs on Linux.
func hostMemoryPercent() (float64, bool) {
	var stat memoryStatusEx
	stat.Length = uint32(unsafe.Sizeof(stat))
	ret, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&stat)))
	if ret == 0 {
		return 0, false
	}
	return float64(stat.MemoryLoad), true
}

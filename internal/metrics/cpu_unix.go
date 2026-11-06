//go:build !windows

package metrics

import "syscall"

func processCPUSeconds() (float64, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	userSec := float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1_000_000.0
	sysSec := float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1_000_000.0
	return userSec + sysSec, true
}

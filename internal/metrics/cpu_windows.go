//go:build windows

package metrics

import "syscall"

func processCPUSeconds() (float64, bool) {
	handle, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0, false
	}

	var creation syscall.Filetime
	var exit syscall.Filetime
	var kernel syscall.Filetime
	var user syscall.Filetime
	if err := syscall.GetProcessTimes(handle, &creation, &exit, &kernel, &user); err != nil {
		return 0, false
	}

	// FILETIME is 100ns units since Jan 1, 1601.
	// CPU time is user + kernel process time.
	userSec := float64(user.Nanoseconds()) / 1_000_000_000.0
	kernelSec := float64(kernel.Nanoseconds()) / 1_000_000_000.0
	return userSec + kernelSec, true
}

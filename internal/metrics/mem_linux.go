//go:build linux

package metrics

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// hostMemoryPercent reads /proc/meminfo and returns used physical memory as a
// percent of total (0-100). Used is derived as Total - Available, which tracks
// what an operator perceives as memory pressure better than Total - Free.
func hostMemoryPercent() (float64, bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer f.Close()

	var total, avail float64
	var haveTotal, haveAvail bool
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			if v, err := strconv.ParseFloat(fields[1], 64); err == nil {
				total, haveTotal = v, true
			}
		case "MemAvailable:":
			if v, err := strconv.ParseFloat(fields[1], 64); err == nil {
				avail, haveAvail = v, true
			}
		}
		if haveTotal && haveAvail {
			break
		}
	}
	if !haveTotal || !haveAvail || total <= 0 {
		return 0, false
	}

	pct := (total - avail) / total * 100
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return pct, true
}

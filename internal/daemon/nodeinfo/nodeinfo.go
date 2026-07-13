// Package nodeinfo detects a node's static capacity (CPU / memory / disk) for
// registration in state. Detection is best-effort: on exotic platforms any
// probe that fails falls back to zero rather than crashing the daemon.
package nodeinfo

import (
	"log/slog"
	"runtime"

	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

// Capacity is a node's schedulable resources.
type Capacity struct {
	CPUMillis uint32 // logical CPUs × 1000
	MemoryMB  uint32
	DiskMB    uint32
}

// Detect probes CPU, memory and the disk holding dataDir. Failures degrade to
// zero for that dimension (logged), never a panic.
func Detect(dataDir string, log *slog.Logger) Capacity {
	if log == nil {
		log = slog.Default()
	}
	cap := Capacity{CPUMillis: uint32(runtime.NumCPU()) * 1000}

	if vm, err := mem.VirtualMemory(); err != nil {
		log.Warn("node memory detection failed; reporting 0", "err", err)
	} else {
		cap.MemoryMB = uint32(vm.Total >> 20)
	}

	if du, err := disk.Usage(dataDir); err != nil {
		log.Warn("node disk detection failed; reporting 0", "err", err, "dir", dataDir)
	} else {
		cap.DiskMB = uint32(du.Total >> 20)
	}
	return cap
}

//go:build cloud

package cloud

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/cloud/provider"
)

// TestCloudListTypes is a READ-ONLY diagnostic (creates nothing): it prints the
// cheapest orderable amd64/arm64 server type per region, so you can pick a
// working ZT_CLOUD_REGION / ZT_CLOUD_*_TYPE without trial-and-error creates.
//
//	HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestCloudListTypes -v
func TestCloudListTypes(t *testing.T) {
	token := os.Getenv("HCLOUD_TOKEN")
	if token == "" {
		t.Skip("cloud: set HCLOUD_TOKEN to list server types")
	}
	d := provider.NewHetzner(token, os.Getenv("ZT_CLOUD_API"))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	regions := []string{"nbg1", "fsn1", "hel1", "ash", "hil", "sin"}
	if r := os.Getenv("ZT_CLOUD_REGION"); r != "" {
		regions = append([]string{r}, regions...)
	}
	for _, region := range regions {
		types, err := d.AvailableServerTypes(ctx, region)
		if err != nil {
			t.Logf("%-5s error: %v", region, err)
			continue
		}
		if len(types) == 0 {
			t.Logf("%-5s (no orderable types — location may be unavailable to this account)", region)
			continue
		}
		byArch := map[string][]provider.ServerTypeInfo{}
		for _, st := range types {
			byArch[st.Arch] = append(byArch[st.Arch], st)
		}
		for _, arch := range []string{"amd64", "arm64"} {
			list := byArch[arch]
			sort.Slice(list, func(i, j int) bool { return list[i].HourlyPriceEUR < list[j].HourlyPriceEUR })
			if len(list) == 0 {
				t.Logf("%-5s %-6s: none orderable", region, arch)
				continue
			}
			c := list[0]
			t.Logf("%-5s %-6s: cheapest=%s (%d vCPU, %.0fGB, €%.4f/h) — %d orderable", region, arch, c.Name, c.Cores, c.MemoryGB, c.HourlyPriceEUR, len(list))
		}
	}
}

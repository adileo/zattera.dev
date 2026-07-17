package cli

import (
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func TestVolumeStatus(t *testing.T) {
	cases := map[zatterav1.VolumeStatus]string{
		zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE:      "active",
		zatterav1.VolumeStatus_VOLUME_STATUS_NODE_LOST:   "node-lost",
		zatterav1.VolumeStatus_VOLUME_STATUS_RESTORING:   "restoring",
		zatterav1.VolumeStatus_VOLUME_STATUS_UNSPECIFIED: "unknown",
	}
	for status, want := range cases {
		if got := volumeStatus(status); got != want {
			t.Errorf("volumeStatus(%v) = %q, want %q", status, got, want)
		}
	}
}

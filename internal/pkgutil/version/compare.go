package version

import (
	"strconv"
	"strings"
)

// Parsed is a build version broken into comparable parts.
//
// Versions are `git describe` output, not clean semver: "v0.3.1",
// "v0.3.1-4-gabc1234" (4 commits past the tag) or "dev" on an untagged build.
// Anything that does not start with a vX.Y.Z tag is Unknown, and an unknown
// version is never treated as older or newer than a known one — guessing there
// would let an upgrade planner skip a node it cannot reason about.
type Parsed struct {
	Major, Minor, Patch int
	// Ahead is the commit count past the tag (the -N- in git describe), so
	// v0.3.1-4-gabc sorts after v0.3.1.
	Ahead   int
	Unknown bool
}

// Parse breaks a build version string into its parts.
func Parse(v string) Parsed {
	raw := strings.TrimSpace(v)
	raw = strings.TrimSuffix(raw, "-dirty")
	raw = strings.TrimPrefix(raw, "v")
	if raw == "" {
		return Parsed{Unknown: true}
	}
	// Split off the git-describe suffix: <tag>-<ahead>-g<sha>.
	core, ahead := raw, 0
	if parts := strings.Split(raw, "-"); len(parts) >= 3 && strings.HasPrefix(parts[len(parts)-1], "g") {
		if n, err := strconv.Atoi(parts[len(parts)-2]); err == nil {
			ahead = n
			core = strings.Join(parts[:len(parts)-2], "-")
		}
	}
	// Drop any prerelease/build metadata: 1.2.3-rc1 compares as 1.2.3.
	core, _, _ = strings.Cut(core, "-")
	core, _, _ = strings.Cut(core, "+")

	nums := strings.Split(core, ".")
	if len(nums) == 0 || nums[0] == "" {
		return Parsed{Unknown: true}
	}
	out := Parsed{Ahead: ahead}
	for i, n := range nums {
		if i > 2 {
			break
		}
		val, err := strconv.Atoi(n)
		if err != nil {
			return Parsed{Unknown: true}
		}
		switch i {
		case 0:
			out.Major = val
		case 1:
			out.Minor = val
		case 2:
			out.Patch = val
		}
	}
	return out
}

// Compare orders two version strings: -1 if a is older, 1 if newer, 0 if equal.
// ok is false when either side is unknown, and callers must not order them.
func Compare(a, b string) (result int, ok bool) {
	pa, pb := Parse(a), Parse(b)
	if pa.Unknown || pb.Unknown {
		return 0, false
	}
	for _, pair := range [][2]int{
		{pa.Major, pb.Major}, {pa.Minor, pb.Minor}, {pa.Patch, pb.Patch}, {pa.Ahead, pb.Ahead},
	} {
		if pair[0] != pair[1] {
			if pair[0] < pair[1] {
				return -1, true
			}
			return 1, true
		}
	}
	return 0, true
}

// Older reports whether a is strictly older than b. An unknown version is not
// older than anything — it is simply not comparable.
func Older(a, b string) bool {
	c, ok := Compare(a, b)
	return ok && c < 0
}

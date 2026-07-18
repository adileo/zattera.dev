package version

import "testing"

// TestParse covers the shapes `git describe` actually emits.
func TestParse(t *testing.T) {
	cases := []struct {
		in    string
		want  Parsed
		known bool
	}{
		{"v0.3.1", Parsed{Major: 0, Minor: 3, Patch: 1}, true},
		{"0.3.1", Parsed{Major: 0, Minor: 3, Patch: 1}, true},
		{"v1.0.0", Parsed{Major: 1}, true},
		{"v0.3.1-4-gabc1234", Parsed{Minor: 3, Patch: 1, Ahead: 4}, true},
		{"v0.3.1-4-gabc1234-dirty", Parsed{Minor: 3, Patch: 1, Ahead: 4}, true},
		{"v0.4", Parsed{Minor: 4}, true},
		{"v1.2.3-rc1", Parsed{Major: 1, Minor: 2, Patch: 3}, true},
		{"dev", Parsed{}, false},
		{"", Parsed{}, false},
		{"garbage", Parsed{}, false},
	}
	for _, tc := range cases {
		got := Parse(tc.in)
		if got.Unknown == tc.known {
			t.Errorf("Parse(%q).Unknown = %v, want known=%v", tc.in, got.Unknown, tc.known)
			continue
		}
		if !tc.known {
			continue
		}
		if got.Major != tc.want.Major || got.Minor != tc.want.Minor || got.Patch != tc.want.Patch || got.Ahead != tc.want.Ahead {
			t.Errorf("Parse(%q) = %+v, want %+v", tc.in, got, tc.want)
		}
	}
}

// TestCompare pins the ordering, including the git-describe "ahead" counter
// that makes an untagged build sort after the tag it descends from.
func TestCompare(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"v0.3.1", "v0.3.2", -1},
		{"v0.3.2", "v0.3.1", 1},
		{"v0.3.1", "v0.3.1", 0},
		{"v0.3.1", "0.3.1", 0},
		{"v0.9.9", "v1.0.0", -1},
		{"v0.3.1", "v0.3.1-2-gabc", -1}, // commits past the tag are newer
		{"v0.3.1-1-gabc", "v0.3.1-2-gdef", -1},
		{"v0.10.0", "v0.9.0", 1}, // numeric, not lexical
	}
	for _, tc := range cases {
		got, ok := Compare(tc.a, tc.b)
		if !ok {
			t.Errorf("Compare(%q,%q) not comparable", tc.a, tc.b)
			continue
		}
		if got != tc.want {
			t.Errorf("Compare(%q,%q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// TestCompareUnknown is the safety property: an unparseable version is never
// ordered. Treating "dev" as old would let an upgrade planner silently decide a
// node needs replacing — or worse, that it does not.
func TestCompareUnknown(t *testing.T) {
	for _, pair := range [][2]string{{"dev", "v1.0.0"}, {"v1.0.0", "dev"}, {"", "v1.0.0"}, {"dev", "dev"}} {
		if _, ok := Compare(pair[0], pair[1]); ok {
			t.Errorf("Compare(%q,%q) reported comparable", pair[0], pair[1])
		}
		if Older(pair[0], pair[1]) {
			t.Errorf("Older(%q,%q) = true; unknown versions must not be ordered", pair[0], pair[1])
		}
	}
}

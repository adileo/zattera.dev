package cli

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// writeSourceTar writes a deterministic gzipped tar of dir to w, honouring
// .gitignore and .zatteraignore and always excluding .git. Determinism (same
// tree → byte-identical archive) matters so repeat uploads dedupe by digest:
// entries are sorted, timestamps zeroed, uid/gid set to 0 and ownership names
// cleared, and the USTAR format avoids PAX records (no atime/xattrs).
func writeSourceTar(dir string, w io.Writer) error {
	rules := loadIgnore(dir)

	var files []string
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel(dir, p)
		if rerr != nil {
			return rerr
		}
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if rules.ignored(rel, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil // skip symlinks (non-deterministic, traversal risk)
		}
		if d.Type().IsRegular() {
			files = append(files, rel)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(files)

	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	for _, rel := range files {
		if err := writeTarFile(tw, dir, rel); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

func writeTarFile(tw *tar.Writer, dir, rel string) error {
	full := filepath.Join(dir, rel)
	fi, err := os.Stat(full)
	if err != nil {
		return err
	}
	hdr := &tar.Header{
		Name:     rel,
		Mode:     int64(fi.Mode().Perm()),
		Size:     fi.Size(),
		Typeflag: tar.TypeReg,
		ModTime:  time.Unix(0, 0).UTC(), // zeroed for determinism
		Uid:      0,
		Gid:      0,
		Format:   tar.FormatUSTAR, // no PAX records → no atime/ctime/xattrs
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar header %s: %w", rel, err)
	}
	f, err := os.Open(full)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(tw, f); err != nil {
		return fmt.Errorf("tar copy %s: %w", rel, err)
	}
	return nil
}

// ignoreRules is an ordered set of .gitignore-style rules.
type ignoreRules struct{ rules []ignoreRule }

type ignoreRule struct {
	pattern  string
	negate   bool
	dirOnly  bool
	anchored bool
}

// loadIgnore reads .gitignore and .zatteraignore from dir and always excludes
// .git. Nested ignore files are not consulted (v1).
func loadIgnore(dir string) ignoreRules {
	r := ignoreRules{rules: []ignoreRule{{pattern: ".git", dirOnly: true}}}
	for _, name := range []string{".gitignore", ".zatteraignore"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			if rule, ok := parseIgnoreLine(line); ok {
				r.rules = append(r.rules, rule)
			}
		}
	}
	return r
}

func parseIgnoreLine(line string) (ignoreRule, bool) {
	line = strings.TrimRight(line, "\r")
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return ignoreRule{}, false
	}
	rule := ignoreRule{}
	if strings.HasPrefix(line, "!") {
		rule.negate = true
		line = line[1:]
	}
	if strings.HasSuffix(line, "/") {
		rule.dirOnly = true
		line = strings.TrimSuffix(line, "/")
	}
	if strings.HasPrefix(line, "/") {
		rule.anchored = true
		line = strings.TrimPrefix(line, "/")
	}
	if line == "" {
		return ignoreRule{}, false
	}
	rule.pattern = line
	return rule, true
}

// ignored reports whether rel (slash-separated, relative to the root) is
// excluded. Later rules override earlier ones; a negation re-includes.
func (r ignoreRules) ignored(rel string, isDir bool) bool {
	out := false
	for _, rule := range r.rules {
		if rule.dirOnly && !isDir {
			continue
		}
		if rule.matches(rel) {
			out = !rule.negate
		}
	}
	return out
}

func (rule ignoreRule) matches(rel string) bool {
	if strings.Contains(rule.pattern, "/") || rule.anchored {
		if ok, _ := path.Match(rule.pattern, rel); ok {
			return true
		}
		// A directory pattern also matches everything beneath it.
		return strings.HasPrefix(rel, rule.pattern+"/")
	}
	// A pattern without a slash matches a path component at any depth, and
	// everything under a matching directory component.
	for _, seg := range strings.Split(rel, "/") {
		if ok, _ := path.Match(rule.pattern, seg); ok {
			return true
		}
	}
	return false
}

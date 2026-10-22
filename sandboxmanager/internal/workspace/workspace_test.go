package workspace

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// newRoot returns a workspace inside a temp dir, plus a "secret" file next
// to it (outside the workspace) that no operation may reach.
func newRoot(t *testing.T) (*os.Root, string) {
	t.Helper()
	base := t.TempDir()
	ws := filepath.Join(base, "ws")
	if err := os.Mkdir(ws, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "secret"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(ws)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	return root, ws
}

func TestRel(t *testing.T) {
	for in, want := range map[string]string{
		"": "", "/": "", "a/b": "a/b", "/workspace": "", "/workspace/a": "a",
		"../../etc/passwd": "etc/passwd", "a/../../b": "b", "workspace/x": "x", "./a//b/": "a/b",
	} {
		if got := Rel(in); got != want {
			t.Errorf("Rel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPutOpenListRemove(t *testing.T) {
	root, _ := newRoot(t)
	n, sum, err := Put(root, "d/e/f.txt", strings.NewReader("hello"))
	if err != nil || n != 5 || len(sum) != 64 {
		t.Fatalf("Put = %d %q %v", n, sum, err)
	}
	f, size, err := Open(root, "d/e/f.txt")
	if err != nil || size != 5 {
		t.Fatalf("Open = %d %v", size, err)
	}
	b, _ := io.ReadAll(f)
	f.Close()
	if string(b) != "hello" {
		t.Fatalf("read %q", b)
	}
	if _, _, err := Put(root, "d", strings.NewReader("x")); !errors.Is(err, ErrIsDir) {
		t.Fatalf("Put over a dir: %v", err)
	}
	if _, _, err := Open(root, "d"); !errors.Is(err, ErrIsDir) {
		t.Fatalf("Open dir: %v", err)
	}
	entries, err := List(root, "d/e")
	if err != nil || len(entries) != 1 || entries[0].Name != "f.txt" || entries[0].Type != "file" || entries[0].Size != 5 {
		t.Fatalf("List = %+v %v", entries, err)
	}
	if _, err := List(root, "d/e/f.txt"); !errors.Is(err, ErrNotDir) {
		t.Fatalf("List file: %v", err)
	}
	if _, err := List(root, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("List missing: %v", err)
	}
	if err := Remove(root, "d"); err != nil {
		t.Fatal(err)
	}
	if err := Remove(root, "d"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Remove twice: %v", err)
	}
}

func TestSymlinksStayInside(t *testing.T) {
	root, ws := newRoot(t)
	os.Symlink("../secret", filepath.Join(ws, "up"))
	os.Symlink(filepath.Join(filepath.Dir(ws), "secret"), filepath.Join(ws, "abs"))
	os.Symlink("..", filepath.Join(ws, "parent"))
	os.WriteFile(filepath.Join(ws, "ok.txt"), []byte("ok"), 0o644)
	os.Symlink("ok.txt", filepath.Join(ws, "inner"))

	for _, p := range []string{"up", "abs", "parent/secret"} {
		if _, _, err := Open(root, p); !errors.Is(err, ErrEscapes) && !errors.Is(err, ErrNotFound) {
			t.Errorf("Open(%q): %v, want escape refused", p, err)
		}
		if _, err := List(root, p); err == nil {
			t.Errorf("List(%q) succeeded", p)
		}
	}
	// Writing through a symlinked directory that leaves the workspace fails.
	if _, _, err := Put(root, "parent/new", strings.NewReader("x")); err == nil {
		t.Error("Put through parent symlink succeeded")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(ws), "new")); err == nil {
		t.Error("file created outside the workspace")
	}
	// Symlinks inside the workspace work.
	f, _, err := Open(root, "inner")
	if err != nil {
		t.Fatalf("inner symlink: %v", err)
	}
	f.Close()
	// Put on a symlink replaces the link, never the target outside.
	if _, _, err := Put(root, "up", strings.NewReader("pwned")); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(filepath.Dir(ws), "secret")); string(b) != "secret" {
		t.Fatal("secret overwritten")
	}
	// Remove on a symlink removes the link only.
	if err := Remove(root, "parent"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(ws), "secret")); err != nil {
		t.Fatal("remove followed the symlink")
	}
}

func tarOf(t *testing.T, entries ...*tar.Header) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, h := range entries {
		body := ""
		if h.Typeflag == tar.TypeReg {
			body = h.Linkname // test convenience: content in Linkname
			h.Size, h.Linkname = int64(len(body)), ""
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		io.WriteString(tw, body)
	}
	tw.Close()
	return &buf
}

func TestUploadRejectsBadEntries(t *testing.T) {
	for _, h := range []*tar.Header{
		{Typeflag: tar.TypeReg, Name: "../evil", Mode: 0o644},
		{Typeflag: tar.TypeReg, Name: "/abs", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "link", Linkname: "/etc/passwd"},
		{Typeflag: tar.TypeLink, Name: "hard", Linkname: "x"},
		{Typeflag: tar.TypeFifo, Name: "fifo"},
	} {
		root, ws := newRoot(t)
		_, _, err := Extract(root, "", tarOf(t, h), Upload)
		var bad *BadEntryError
		if !errors.As(err, &bad) {
			t.Errorf("%s %q: err = %v, want BadEntryError", string(h.Typeflag), h.Name, err)
		}
		if _, err := os.Lstat(filepath.Join(filepath.Dir(ws), "evil")); err == nil {
			t.Error("wrote outside the workspace")
		}
	}
}

func TestUploadThroughEscapingSymlinkFails(t *testing.T) {
	root, ws := newRoot(t)
	os.Symlink("..", filepath.Join(ws, "out"))
	_, _, err := Extract(root, "", tarOf(t, &tar.Header{Typeflag: tar.TypeReg, Name: "out/evil", Mode: 0o644, Linkname: "x"}), Upload)
	if err == nil {
		t.Fatal("upload through symlink succeeded")
	}
	if _, err := os.Lstat(filepath.Join(filepath.Dir(ws), "evil")); err == nil {
		t.Fatal("wrote outside the workspace")
	}
}

func TestArchiveRestoreRoundTrip(t *testing.T) {
	src, ws := newRoot(t)
	os.MkdirAll(filepath.Join(ws, "a/b"), 0o755)
	os.WriteFile(filepath.Join(ws, "a/b/f"), []byte("data"), 0o640)
	os.WriteFile(filepath.Join(ws, "top"), []byte("t"), 0o600)
	os.Symlink("a/b/f", filepath.Join(ws, "rel-link"))
	os.Symlink("/etc/passwd", filepath.Join(ws, "abs-link"))

	var snap bytes.Buffer
	if err := Archive(src, "", &snap); err != nil {
		t.Fatal(err)
	}
	dst, ws2 := newRoot(t)
	files, n, err := Extract(dst, "", bytes.NewReader(snap.Bytes()), Restore)
	if err != nil || files != 2 || n != 5 {
		t.Fatalf("Extract = %d files %d bytes, %v", files, n, err)
	}
	if b, _ := os.ReadFile(filepath.Join(ws2, "a/b/f")); string(b) != "data" {
		t.Fatalf("content %q", b)
	}
	if fi, _ := os.Stat(filepath.Join(ws2, "a/b/f")); fi.Mode().Perm() != 0o640 {
		t.Fatalf("mode %v", fi.Mode())
	}
	if l, _ := os.Readlink(filepath.Join(ws2, "abs-link")); l != "/etc/passwd" {
		t.Fatalf("abs-link restored as %q", l)
	}
	// ...but still can't be followed through the workspace API.
	if _, _, err := Open(dst, "abs-link"); err == nil {
		t.Fatal("followed an absolute symlink")
	}
}

func TestRestoreSkipsEscapingEntries(t *testing.T) {
	root, ws := newRoot(t)
	snap := tarOf(t,
		&tar.Header{Typeflag: tar.TypeSymlink, Name: "out", Linkname: ".."},
		&tar.Header{Typeflag: tar.TypeReg, Name: "out/evil", Mode: 0o644, Linkname: "x"},
		&tar.Header{Typeflag: tar.TypeReg, Name: "../evil2", Mode: 0o644, Linkname: "x"},
		&tar.Header{Typeflag: tar.TypeReg, Name: "fine", Mode: 0o644, Linkname: "ok"},
	)
	if _, _, err := Extract(root, "", snap, Restore); err == nil {
		// writing out/evil must fail (it would leave the workspace)
		t.Log("restore finished; checking nothing escaped")
	}
	for _, n := range []string{"evil", "evil2"} {
		if _, err := os.Lstat(filepath.Join(filepath.Dir(ws), n)); err == nil {
			t.Fatalf("%s written outside the workspace", n)
		}
	}
}

func snapshotOf(t *testing.T, setup func(ws string)) []byte {
	t.Helper()
	root, ws := newRoot(t)
	setup(ws)
	var buf bytes.Buffer
	if err := Archive(root, "", &buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func names(t *testing.T, r io.Reader) []string {
	t.Helper()
	var out []string
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, h.Name)
	}
	sort.Strings(out)
	return out
}

func TestSnapshotReadListRewrite(t *testing.T) {
	snap := snapshotOf(t, func(ws string) {
		os.MkdirAll(filepath.Join(ws, "a/b"), 0o777)
		os.WriteFile(filepath.Join(ws, "a/b/f"), []byte("data"), 0o644)
		os.WriteFile(filepath.Join(ws, "z"), []byte("zz"), 0o644)
		os.Symlink("z", filepath.Join(ws, "l"))
	})

	var got []byte
	err := ReadSnapshot(bytes.NewReader(snap), "a/b/f", func(size int64, body io.Reader) error {
		got, _ = io.ReadAll(body)
		return nil
	}, nil)
	if err != nil || string(got) != "data" {
		t.Fatalf("read file: %q %v", got, err)
	}
	var dir bytes.Buffer
	err = ReadSnapshot(bytes.NewReader(snap), "a", nil, func() (io.Writer, error) { return &dir, nil })
	if err != nil {
		t.Fatal(err)
	}
	if n := names(t, &dir); strings.Join(n, ",") != "b/,b/f" {
		t.Fatalf("dir tar = %v", n)
	}
	if err := ReadSnapshot(bytes.NewReader(snap), "l", nil, nil); !errors.Is(err, ErrSymlink) {
		t.Fatalf("symlink read: %v", err)
	}
	if err := ReadSnapshot(bytes.NewReader(snap), "nope", nil, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing read: %v", err)
	}
	ls, err := ListSnapshot(bytes.NewReader(snap), "")
	if err != nil || len(ls) != 3 {
		t.Fatalf("list root = %+v %v", ls, err)
	}
	if _, err := ListSnapshot(bytes.NewReader(snap), "z"); !errors.Is(err, ErrNotDir) {
		t.Fatalf("list file: %v", err)
	}

	// Paused write: replace a/b/f, add a/new, delete z.
	var out bytes.Buffer
	err = Rewrite(bytes.NewReader(snap), &out, func(n string) bool { return n == "a/b/f" || IsBelow(n, "z") },
		func(tw *tar.Writer, types map[string]byte) error {
			if _, err := AddFile(tw, types, "a/b/f", 3, strings.NewReader("new")); err != nil {
				return err
			}
			_, err := AddFile(tw, types, "a/new", 1, strings.NewReader("n"))
			return err
		})
	if err != nil {
		t.Fatal(err)
	}
	// Appended entries are found even though they're out of directory order.
	var dir2 bytes.Buffer
	if err := ReadSnapshot(bytes.NewReader(out.Bytes()), "a", nil, func() (io.Writer, error) { return &dir2, nil }); err != nil {
		t.Fatal(err)
	}
	if n := names(t, &dir2); strings.Join(n, ",") != "b/,b/f,new" {
		t.Fatalf("after rewrite = %v", n)
	}
	ReadSnapshot(bytes.NewReader(out.Bytes()), "a/b/f", func(_ int64, b io.Reader) error { got, _ = io.ReadAll(b); return nil }, nil)
	if string(got) != "new" {
		t.Fatalf("rewritten content %q", got)
	}
	// Restoring the rewritten snapshot gives the edited tree.
	root, ws := newRoot(t)
	if _, _, err := Extract(root, "", bytes.NewReader(out.Bytes()), Restore); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(ws, "z")); err == nil {
		t.Fatal("deleted file restored")
	}

	// Conflicts fail the rewrite.
	for _, tc := range []struct {
		rel  string
		want error
	}{{"a", ErrIsDir}, {"z/x", ErrNotDir}, {"l/x", ErrSymlink}} {
		err := Rewrite(bytes.NewReader(snap), io.Discard, func(string) bool { return false },
			func(tw *tar.Writer, types map[string]byte) error {
				_, err := AddFile(tw, types, tc.rel, 1, strings.NewReader("x"))
				return err
			})
		if !errors.Is(err, tc.want) {
			t.Errorf("AddFile(%q) = %v, want %v", tc.rel, err, tc.want)
		}
	}
}

func TestAddUpload(t *testing.T) {
	up := tarOf(t,
		&tar.Header{Typeflag: tar.TypeDir, Name: "d/", Mode: 0o755},
		&tar.Header{Typeflag: tar.TypeReg, Name: "d/f", Mode: 0o644, Linkname: "abc"},
	)
	upBytes := up.Bytes()
	names0, err := UploadNames(bytes.NewReader(upBytes), "sub")
	if err != nil || !names0["sub/d/f"] || len(names0) != 1 {
		t.Fatalf("UploadNames = %v %v", names0, err)
	}
	var out bytes.Buffer
	var files, n int64
	err = Rewrite(nil, &out, func(string) bool { return false }, func(tw *tar.Writer, types map[string]byte) error {
		var err error
		files, n, err = AddUpload(tw, types, "sub", bytes.NewReader(upBytes))
		return err
	})
	if err != nil || files != 1 || n != 3 {
		t.Fatalf("AddUpload = %d %d %v", files, n, err)
	}
	if got := strings.Join(names(t, &out), ","); got != "sub/,sub/d/,sub/d/f" {
		t.Fatalf("names = %s", got)
	}
}

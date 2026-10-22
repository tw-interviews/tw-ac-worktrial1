// Package workspace implements file operations on a sandbox's /workspace:
// on the live directory (used by the node agent) and on tar snapshots of it
// (used by the API for paused and deleted sandboxes).
//
// Workspaces are written by untrusted sandbox code, so every live operation
// goes through an os.Root: symlinks are followed only while they stay inside
// the workspace, and absolute symlinks are refused. Never touch a workspace
// with plain os functions.
package workspace

import (
	"archive/tar"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"syscall"
	"time"
)

// Errors returned by this package; callers map them to HTTP statuses.
var (
	ErrNotFound = errors.New("no such file or directory")
	ErrNotDir   = errors.New("not a directory")
	ErrIsDir    = errors.New("path is a directory")
	ErrEscapes  = errors.New("path escapes the workspace")
	ErrNoSpace  = errors.New("workspace is full")
	ErrSymlink  = errors.New("symlinks can't be followed while the sandbox isn't running")
)

// BadEntryError is an upload tar entry that was rejected.
type BadEntryError struct{ Name, Reason string }

func (e *BadEntryError) Error() string { return fmt.Sprintf("%q: %s", e.Name, e.Reason) }

// Rel maps an API path (relative to /workspace; a leading "/workspace/" is
// accepted too) to a clean path relative to the workspace root: "" for the
// root itself, else e.g. "a/b". Cleaning a rooted path removes every "..".
func Rel(p string) string {
	p = path.Clean("/" + p)
	if p == "/workspace" || strings.HasPrefix(p, "/workspace/") {
		p = strings.TrimPrefix(p, "/workspace")
	}
	return strings.TrimPrefix(p, "/")
}

func osName(rel string) string {
	if rel == "" {
		return "."
	}
	return rel
}

// classify maps filesystem errors to this package's errors.
func classify(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return ErrNotFound
	case errors.Is(err, syscall.ENOTDIR):
		return ErrNotDir
	case errors.Is(err, syscall.EISDIR):
		return ErrIsDir
	case errors.Is(err, syscall.ENOSPC), errors.Is(err, syscall.EDQUOT):
		return ErrNoSpace
	case strings.Contains(err.Error(), "path escapes from parent"):
		// os.Root's error for symlinks (or names) leading outside the root.
		return ErrEscapes
	}
	return err
}

// FileInfo describes one directory entry.
type FileInfo struct {
	Name     string    `json:"name"`
	Type     string    `json:"type"` // file, directory, symlink or other
	Size     int64     `json:"size"`
	Mode     string    `json:"mode"` // octal permission bits, e.g. "644"
	Modified time.Time `json:"modified"`
}

func infoOf(name string, fi fs.FileInfo) FileInfo {
	return FileInfo{Name: name, Type: typeOf(fi.Mode()), Size: fi.Size(),
		Mode: fmt.Sprintf("%o", fi.Mode().Perm()), Modified: fi.ModTime().UTC().Truncate(time.Second)}
}

func typeOf(m fs.FileMode) string {
	switch {
	case m.IsRegular():
		return "file"
	case m.IsDir():
		return "directory"
	case m&fs.ModeSymlink != 0:
		return "symlink"
	}
	return "other"
}

// Stat returns what rel is, following symlinks that stay inside the workspace.
func Stat(root *os.Root, rel string) (fs.FileInfo, error) {
	fi, err := root.Stat(osName(rel))
	return fi, classify(err)
}

// List returns the entries of directory rel.
func List(root *os.Root, rel string) ([]FileInfo, error) {
	fi, err := Stat(root, rel)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, ErrNotDir
	}
	dir, err := root.Open(osName(rel))
	if err != nil {
		return nil, classify(err)
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, classify(err)
	}
	out := make([]FileInfo, 0, len(names))
	for _, n := range names {
		fi, err := root.Lstat(path.Join(osName(rel), n))
		if err != nil {
			continue // removed meanwhile
		}
		out = append(out, infoOf(n, fi))
	}
	return out, nil
}

// Open opens regular file rel for reading.
func Open(root *os.Root, rel string) (*os.File, int64, error) {
	f, err := root.Open(osName(rel))
	if err != nil {
		return nil, 0, classify(err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, classify(err)
	}
	if fi.IsDir() {
		f.Close()
		return nil, 0, ErrIsDir
	}
	return f, fi.Size(), nil
}

// Put writes r to file rel through a temporary file that is renamed into
// place, so readers never see a partial file. Parent directories are
// created. Returns the size and SHA-256 of what was written.
func Put(root *os.Root, rel string, r io.Reader) (int64, string, error) {
	if rel == "" {
		return 0, "", ErrIsDir
	}
	if fi, err := root.Lstat(rel); err == nil && fi.IsDir() {
		return 0, "", ErrIsDir
	}
	dir := path.Dir(rel)
	if err := root.MkdirAll(dir, 0o777); err != nil {
		return 0, "", classify(err)
	}
	tmp := path.Join(dir, ".upload."+randomSuffix())
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return 0, "", classify(err)
	}
	hash := sha256.New()
	n, err := io.Copy(f, io.TeeReader(r, hash))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = root.Rename(tmp, rel)
	}
	if err != nil {
		root.Remove(tmp)
		return 0, "", classify(err)
	}
	return n, hex.EncodeToString(hash.Sum(nil)), nil
}

// Remove deletes rel (recursively for directories; a symlink itself, never
// its target).
func Remove(root *os.Root, rel string) error {
	if _, err := root.Lstat(osName(rel)); err != nil {
		return classify(err)
	}
	return classify(root.RemoveAll(rel))
}

func randomSuffix() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Archive writes directory rel and everything below it to w as a tar
// stream, with names relative to rel (directories end in "/"), owners,
// permissions and symlinks preserved. Parents always come before their
// children. Hard links are stored as separate files; sockets, devices and
// FIFOs are skipped. A file that changes size while being archived is
// truncated or zero-padded to the size it had when its header was written.
func Archive(root *os.Root, rel string, w io.Writer) error {
	sub, err := root.OpenRoot(osName(rel))
	if err != nil {
		return classify(err)
	}
	defer sub.Close()
	tw := tar.NewWriter(w)
	err = fs.WalkDir(sub.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == "." {
				return err
			}
			return nil // vanished meanwhile
		}
		if p == "." {
			return nil
		}
		fi, err := sub.Lstat(p)
		if err != nil {
			return nil
		}
		var link string
		switch {
		case fi.Mode()&fs.ModeSymlink != 0:
			if link, err = sub.Readlink(p); err != nil {
				return nil
			}
		case fi.IsDir(), fi.Mode().IsRegular():
		default:
			return nil
		}
		hdr, err := tar.FileInfoHeader(fi, link)
		if err != nil {
			return err
		}
		hdr.Name, hdr.Uname, hdr.Gname, hdr.Format = p, "", "", 0
		if fi.IsDir() {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !fi.Mode().IsRegular() {
			return nil
		}
		f, err := sub.Open(p)
		if err != nil {
			_, err = io.CopyN(tw, zeros{}, hdr.Size)
			return err
		}
		defer f.Close()
		n, err := io.Copy(tw, io.LimitReader(f, hdr.Size))
		if err != nil {
			return err
		}
		_, err = io.CopyN(tw, zeros{}, hdr.Size-n) // file shrank
		return err
	})
	if err != nil {
		return classify(err)
	}
	return tw.Close()
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// ExtractMode says how much of a tar stream Extract trusts.
type ExtractMode int

const (
	// Upload is a client's upload: only regular files and directories, which
	// get the uploader's permission bits (no setuid/setgid/sticky) and are
	// owned by the agent. Any other entry fails the whole upload.
	Upload ExtractMode = iota
	// Restore is a snapshot made by Archive: symlinks and owners are kept.
	// Entries that would land outside the workspace are skipped.
	Restore
)

// Extract writes a tar stream into directory rel, replacing existing files
// (but not directories) of the same name. Returns the number of regular
// files and bytes written.
func Extract(root *os.Root, rel string, r io.Reader, mode ExtractMode) (files, bytes int64, err error) {
	dst, err := root.OpenRoot(osName(rel))
	if err != nil {
		return 0, 0, classify(err)
	}
	defer dst.Close()
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return files, bytes, nil
		}
		if err != nil {
			return files, bytes, err
		}
		name, ok := entryName(hdr.Name)
		if !ok {
			if mode == Upload {
				return files, bytes, &BadEntryError{hdr.Name, "names must stay inside the workspace"}
			}
			continue
		}
		if name == "" {
			continue // the directory itself
		}
		perm := os.FileMode(hdr.Mode).Perm()
		if mode == Restore {
			perm = os.FileMode(hdr.Mode) & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := dst.MkdirAll(name, 0o777); err != nil {
				return files, bytes, classify(err)
			}
			dst.Chmod(name, perm)
		case tar.TypeReg:
			n, err := extractFile(dst, name, tr, perm)
			bytes += n
			if err != nil {
				return files, bytes, err
			}
			files++
		case tar.TypeSymlink, tar.TypeLink:
			if mode == Upload {
				return files, bytes, &BadEntryError{hdr.Name, "only regular files and directories can be uploaded"}
			}
			if err := replaceable(dst, name); err != nil {
				continue
			}
			if hdr.Typeflag == tar.TypeSymlink {
				err = dst.Symlink(hdr.Linkname, name)
			} else if target, ok := entryName(hdr.Linkname); ok && target != "" {
				err = dst.Link(target, name)
			}
			if err != nil {
				continue // e.g. a link to a skipped entry
			}
		default:
			if mode == Upload {
				return files, bytes, &BadEntryError{hdr.Name, "only regular files and directories can be uploaded"}
			}
			continue
		}
		if mode == Restore {
			dst.Lchown(name, hdr.Uid, hdr.Gid)
			if hdr.Typeflag == tar.TypeReg {
				dst.Chtimes(name, hdr.ModTime, hdr.ModTime)
			}
		}
	}
}

// entryName cleans a tar entry name to a path relative to the extraction
// root ("" for the root), or ok=false if it's absolute or leaves the root.
func entryName(n string) (string, bool) {
	if strings.HasPrefix(n, "/") {
		return "", false
	}
	c := path.Clean(n)
	if c == "." {
		return "", true
	}
	if c == ".." || strings.HasPrefix(c, "../") {
		return "", false
	}
	return c, true
}

// replaceable removes whatever non-directory is at name so a new entry can
// take its place; a directory there is ErrIsDir.
func replaceable(dst *os.Root, name string) error {
	if err := dst.MkdirAll(path.Dir(name), 0o777); err != nil {
		return classify(err)
	}
	fi, err := dst.Lstat(name)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return classify(err)
	case fi.IsDir():
		return ErrIsDir
	}
	return classify(dst.Remove(name))
}

func extractFile(dst *os.Root, name string, r io.Reader, perm os.FileMode) (int64, error) {
	if err := replaceable(dst, name); err != nil {
		return 0, err
	}
	f, err := dst.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, classify(err)
	}
	n, err := io.Copy(f, r)
	if err == nil {
		err = f.Chmod(perm)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return n, classify(err)
}

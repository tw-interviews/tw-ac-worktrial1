package workspace

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

// Operations on snapshots: tar streams written by Archive for the whole
// workspace. Entries have clean relative names, directories end in "/", and
// a directory always comes before its contents, so each operation is a
// single streaming pass. Symlinks in a snapshot are never followed.

func snapName(n string) string { return strings.TrimSuffix(strings.TrimPrefix(n, "./"), "/") }

// ReadSnapshot finds rel in the snapshot. For a regular file it calls file
// with its size and contents. For a directory (rel "" is the root) it calls
// dir, then writes everything below it to the returned writer as a tar
// stream with names relative to rel.
func ReadSnapshot(r io.Reader, rel string, file func(size int64, body io.Reader) error, dir func() (io.Writer, error)) error {
	tr := tar.NewReader(r)
	var tw *tar.Writer
	if rel == "" {
		w, err := dir()
		if err != nil {
			return err
		}
		tw = tar.NewWriter(w)
	}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := snapName(hdr.Name)
		if tw == nil {
			if name != rel {
				continue
			}
			switch hdr.Typeflag {
			case tar.TypeReg:
				return file(hdr.Size, tr)
			case tar.TypeDir:
				w, err := dir()
				if err != nil {
					return err
				}
				tw = tar.NewWriter(w)
				continue
			case tar.TypeSymlink:
				return ErrSymlink
			default:
				return ErrNotFound
			}
		}
		// No early exit: rewrites append entries at the end, so a directory's
		// contents aren't necessarily contiguous.
		sub, ok := below(name, rel)
		if !ok {
			continue
		}
		h := *hdr
		h.Name = sub
		if hdr.Typeflag == tar.TypeDir {
			h.Name += "/"
		}
		if err := tw.WriteHeader(&h); err != nil {
			return err
		}
		if _, err := io.Copy(tw, tr); err != nil {
			return err
		}
	}
	if tw == nil {
		return ErrNotFound
	}
	return tw.Close()
}

// below returns name relative to directory rel, if it's inside it.
func below(name, rel string) (string, bool) {
	if rel == "" {
		return name, name != ""
	}
	if s, ok := strings.CutPrefix(name, rel+"/"); ok {
		return s, true
	}
	return "", false
}

// ListSnapshot returns the entries of directory rel in the snapshot.
func ListSnapshot(r io.Reader, rel string) ([]FileInfo, error) {
	tr := tar.NewReader(r)
	found := rel == ""
	out := []FileInfo{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		name := snapName(hdr.Name)
		if name == rel && rel != "" {
			switch hdr.Typeflag {
			case tar.TypeDir:
				found = true
			case tar.TypeSymlink:
				return nil, ErrSymlink
			default:
				return nil, ErrNotDir
			}
			continue
		}
		sub, ok := below(name, rel)
		if !ok || strings.Contains(sub, "/") {
			continue
		}
		out = append(out, infoOf(sub, hdr.FileInfo()))
	}
	if !found {
		return nil, ErrNotFound
	}
	return out, nil
}

// Rewrite copies snapshot old (nil for an empty workspace) to w, leaving out
// the entries drop returns true for, then calls add to append new entries.
// add gets the type of every entry that was in old, by name, to check for
// conflicts; an error from add (or drop's caller) fails the whole rewrite,
// so the caller must discard w.
func Rewrite(old io.Reader, w io.Writer, drop func(name string) bool, add func(tw *tar.Writer, types map[string]byte) error) error {
	tw := tar.NewWriter(w)
	types := map[string]byte{}
	if old != nil {
		tr := tar.NewReader(old)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			name := snapName(hdr.Name)
			if name == "" {
				continue
			}
			types[name] = hdr.Typeflag
			if drop(name) {
				continue
			}
			h := *hdr
			h.Name = name
			if hdr.Typeflag == tar.TypeDir {
				h.Name += "/"
			}
			if err := tw.WriteHeader(&h); err != nil {
				return err
			}
			if _, err := io.Copy(tw, tr); err != nil {
				return err
			}
		}
	}
	if err := add(tw, types); err != nil {
		return err
	}
	return tw.Close()
}

// AddParents makes sure every directory above rel exists in a rewritten
// snapshot: it appends entries for missing ones (recording them in types),
// and fails if one of them is a file or symlink.
func AddParents(tw *tar.Writer, types map[string]byte, rel string) error {
	parts := strings.Split(rel, "/")
	for i := 1; i < len(parts); i++ {
		dir := strings.Join(parts[:i], "/")
		switch t, ok := types[dir]; {
		case !ok:
			if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: dir + "/", Mode: 0o777, ModTime: time.Now()}); err != nil {
				return err
			}
			types[dir] = tar.TypeDir
		case t == tar.TypeSymlink:
			return ErrSymlink
		case t != tar.TypeDir:
			return ErrNotDir
		}
	}
	return nil
}

// AddFile appends regular file rel with size bytes from r (which must
// supply exactly that many), after its parents. Returns its SHA-256.
func AddFile(tw *tar.Writer, types map[string]byte, rel string, size int64, r io.Reader) (string, error) {
	if t, ok := types[rel]; ok && t == tar.TypeDir {
		return "", ErrIsDir
	}
	if err := AddParents(tw, types, rel); err != nil {
		return "", err
	}
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: rel, Mode: 0o666, Size: size, ModTime: time.Now()}); err != nil {
		return "", err
	}
	hash := sha256.New()
	if _, err := io.CopyN(tw, io.TeeReader(r, hash), size); err != nil {
		return "", err
	}
	types[rel] = tar.TypeReg
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// AddUpload appends the entries of a client's upload (validated as by
// Extract in Upload mode) below directory rel. Returns the regular files'
// count and bytes.
func AddUpload(tw *tar.Writer, types map[string]byte, rel string, upload io.Reader) (files, bytes int64, err error) {
	tr := tar.NewReader(upload)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return files, bytes, nil
		}
		if err != nil {
			return files, bytes, err
		}
		n, ok := entryName(hdr.Name)
		if !ok {
			return files, bytes, &BadEntryError{hdr.Name, "names must stay inside the workspace"}
		}
		name := path.Join(rel, n)
		if n == "" {
			continue
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if t, ok := types[name]; ok && t != tar.TypeDir {
				return files, bytes, ErrNotDir
			}
			if err := AddParents(tw, types, name); err != nil {
				return files, bytes, err
			}
			if _, ok := types[name]; !ok {
				h := &tar.Header{Typeflag: tar.TypeDir, Name: name + "/", Mode: int64(os.FileMode(hdr.Mode).Perm()), ModTime: hdr.ModTime}
				if err := tw.WriteHeader(h); err != nil {
					return files, bytes, err
				}
				types[name] = tar.TypeDir
			}
		case tar.TypeReg:
			if _, err := AddFile(tw, types, name, hdr.Size, tr); err != nil {
				return files, bytes, err
			}
			files++
			bytes += hdr.Size
		default:
			return files, bytes, &BadEntryError{hdr.Name, "only regular files and directories can be uploaded"}
		}
	}
}

// UploadNames returns the names (relative to the workspace) of the regular
// files in an upload below rel, which a rewrite must drop from the old
// snapshot before AddUpload appends them.
func UploadNames(upload io.Reader, rel string) (map[string]bool, error) {
	names := map[string]bool{}
	tr := tar.NewReader(upload)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return names, nil
		}
		if err != nil {
			return nil, err
		}
		if n, ok := entryName(hdr.Name); ok && n != "" && hdr.Typeflag == tar.TypeReg {
			names[path.Join(rel, n)] = true
		}
	}
}

// IsBelow reports whether name is rel or inside it.
func IsBelow(name, rel string) bool {
	return rel == "" || name == rel || strings.HasPrefix(name, rel+"/")
}

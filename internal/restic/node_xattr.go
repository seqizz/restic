//go:build darwin || freebsd || linux || solaris
// +build darwin freebsd linux solaris

package restic

import (
	"os"
	"syscall"

	"github.com/restic/restic/internal/errors"

	"github.com/pkg/xattr"
)

// Getxattr retrieves extended attribute data associated with path.
func Getxattr(path, name string) ([]byte, error) {
	b, err := xattr.LGet(path, name)
	return b, handleXattrErr(err)
}

// Listxattr retrieves a list of names of extended attributes associated with the
// given path in the file system.
func Listxattr(path string) ([]string, error) {
	l, err := xattr.LList(path)
	return l, handleXattrErr(err)
}

func IsListxattrPermissionError(err error) bool {
	var xerr *xattr.Error
	if errors.As(err, &xerr) {
		return xerr.Op == "xattr.list" && errors.Is(xerr.Err, os.ErrPermission)
	}
	return false
}

// Setxattr associates name and data together as an attribute of path.
func Setxattr(path, name string, data []byte) error {
	return handleXattrErr(xattr.LSet(path, name, data))
}

func handleXattrErr(err error) error {
	switch e := err.(type) {
	case nil:
		return nil

	case *xattr.Error:
		// On Linux, xattr calls on files in an SMB/CIFS mount can return
		// ENOATTR instead of ENOTSUP.
		switch e.Err {
		case syscall.ENOTSUP, xattr.ENOATTR:
			return nil
		}
		return errors.WithStack(e)

	default:
		return errors.WithStack(e)
	}
}

// restoreGenericAttributes is no-op.
func (node *Node) restoreGenericAttributes(_ string, warn func(msg string)) error {
	return node.handleAllUnknownGenericAttributesFound(warn)
}

// fillGenericAttributes is a no-op.
func (node *Node) fillGenericAttributes(_ string, _ os.FileInfo, _ *statT) (allowExtended bool, err error) {
	return true, nil
}

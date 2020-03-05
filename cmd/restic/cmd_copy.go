package main

import (
	"context"
	"fmt"
	"os"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"

	"github.com/spf13/cobra"
)

var cmdCopy = &cobra.Command{
	Use:   "copy [flags] [snapshotID ...]",
	Short: "Copy snapshots from one repository to another",
	Long: `
The "copy" command copies one or more snapshots from one repository to another
repository. Note that this will have to read(download) and write(upload) the
entire snapshot(s) due to the different encryption keys on the source and
destination.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCopy(copyOptions, globalOptions, args)
	},
}

// CopyOptions bundles all options for the copy command.
type CopyOptions struct {
	Repo            string
	PasswordFile    string
	PasswordCommand string
	KeyHint         string
	Password        string
	Host            []string
	Tags            restic.TagLists
	Paths           []string
}

var copyOptions CopyOptions

func init() {
	cmdRoot.AddCommand(cmdCopy)

	f := cmdCopy.Flags()
	f.StringVarP(&copyOptions.Repo, "repo2", "", os.Getenv("RESTIC_REPOSITORY2"), "repository to backup to or restore from (default: $RESTIC_REPOSITORY2)")
	f.StringVarP(&copyOptions.PasswordFile, "password-file2", "", os.Getenv("RESTIC_PASSWORD_FILE2"), "read the destination repository password from a file (default: $RESTIC_PASSWORD_FILE2)")
	f.StringVarP(&copyOptions.KeyHint, "key-hint2", "", os.Getenv("RESTIC_KEY_HINT2"), "key ID of key to try decrypting the destination repository first (default: $RESTIC_KEY_HINT2)")
	f.StringVarP(&copyOptions.PasswordCommand, "password-command2", "", os.Getenv("RESTIC_PASSWORD_COMMAND2"), "specify a shell command to obtain a password for the destination repository (default: $RESTIC_PASSWORD_COMMAND2)")

	f.StringArrayVarP(&tagOptions.Hosts, "host", "H", nil, "only consider snapshots for this `host`, when no snapshot ID is given (can be specified multiple times)")
	f.Var(&copyOptions.Tags, "tag", "only consider snapshots which include this `taglist`, when no snapshot-ID is given")
	f.StringArrayVar(&copyOptions.Paths, "path", nil, "only consider snapshots which include this (absolute) `path`, when no snapshot-ID is given")
}

func runCopy(opts CopyOptions, gopts GlobalOptions, args []string) error {
	if opts.Repo == "" {
		return errors.Fatal("Please specify a destination repository location (-repo2)")
	}
	var err error
	dstGopts := gopts
	dstGopts.Repo = opts.Repo
	dstGopts.PasswordFile = opts.PasswordFile
	dstGopts.KeyHint = opts.KeyHint
	dstGopts.password, err = resolvePassword(dstGopts, "RESTIC_PASSWORD2")
	if err != nil {
		return err
	}
	dstGopts.password, err = ReadPassword(dstGopts, "enter password for destination repository: ")
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(gopts.ctx)
	defer cancel()

	srcRepo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	dstRepo, err := OpenRepository(dstGopts)
	if err != nil {
		return err
	}

	srcLock, err := lockRepo(srcRepo)
	defer unlockRepo(srcLock)
	if err != nil {
		return err
	}

	dstLock, err := lockRepo(dstRepo)
	defer unlockRepo(dstLock)
	if err != nil {
		return err
	}

	debug.Log("Loading source index")
	if err := srcRepo.LoadIndex(ctx); err != nil {
		return err
	}

	debug.Log("Loading destination index")
	if err := dstRepo.LoadIndex(ctx); err != nil {
		return err
	}

	for sn := range FindFilteredSnapshots(ctx, srcRepo, opts.Host, opts.Tags, opts.Paths, args) {
		Verbosef("snapshot %s of %v at %s)\n", sn.ID().Str(), sn.Paths, sn.Time)
		Verbosef("  copy started, this may take a while...\n")

		if err := copySnapshot(ctx, srcRepo, dstRepo, *sn.Tree); err != nil {
			return err
		}
		debug.Log("tree copied")

		if err = dstRepo.Flush(ctx); err != nil {
			return err
		}
		debug.Log("flushed packs")

		err = dstRepo.SaveIndex(ctx)
		if err != nil {
			debug.Log("error saving index: %v", err)
			return err
		}
		debug.Log("saved index")

		// save snapshot
		sn.Parent = nil   // does Parent have relevance in the new repo?
		sn.Original = nil // does Original have relevance in the new repo?
		newid, err := dstRepo.SaveJSONUnpacked(ctx, restic.SnapshotFile, sn)
		if err != nil {
			return err
		}
		Verbosef("snapshot %s saved\n", newid.Str())
	}
	return nil
}

func copySnapshot(ctx context.Context, srcRepo, dstRepo restic.Repository, treeID restic.ID) error {
	tree, err := srcRepo.LoadTree(ctx, treeID)
	if err != nil {
		return fmt.Errorf("LoadTree(%v) returned error %v", treeID.Str(), err)
	}
	// Do we already have this tree blob?
	if !dstRepo.Index().Has(treeID, restic.TreeBlob) {
		newtreeid, err := dstRepo.SaveTree(ctx, tree)
		if err != nil {
			return fmt.Errorf("SaveTree(%v) returned error %v", treeID.Str(), err)
		}
		// Assurance only.
		if newtreeid != treeID {
			return fmt.Errorf("SaveTree(%v) returned unexpected id %s", treeID.Str(), newtreeid.Str())
		}
	}

	// TODO: keep only one (big) buffer around.
	// TODO: parellize this stuff, likely only needed inside a tree.

	for _, entry := range tree.Nodes {
		// If it is a directory, recurse
		if entry.Type == "dir" && entry.Subtree != nil {
			if err := copySnapshot(ctx, srcRepo, dstRepo, *entry.Subtree); err != nil {
				return err
			}
		}
		// Copy the blobs for this file.
		for _, blobid := range entry.Content {
			// Do we already have this data blob?
			if dstRepo.Index().Has(blobid, restic.DataBlob) {
				continue
			}
			debug.Log("Copying blob %s\n", blobid.Str())
			size, found := srcRepo.LookupBlobSize(blobid, restic.DataBlob)
			if !found {
				return fmt.Errorf("LookupBlobSize(%v) failed.", blobid)
			}
			buf := restic.NewBlobBuffer(int(size))
			n, err := srcRepo.LoadBlob(ctx, restic.DataBlob, blobid, buf)
			if err != nil {
				return fmt.Errorf("LoadBlob(%v) returned error %v", blobid, err)
			}
			if n != len(buf) {
				return fmt.Errorf("wrong number of bytes read, want %d, got %d", len(buf), n)
			}

			newblobid, err := dstRepo.SaveBlob(ctx, restic.DataBlob, buf, blobid)
			if err != nil {
				return fmt.Errorf("SaveBlob(%v) returned error %v", blobid, err)
			}
			// Assurance only.
			if newblobid != blobid {
				return fmt.Errorf("SaveBlob(%v) returned unexpected id %s", blobid.Str(), newblobid.Str())
			}
		}
	}

	return nil
}

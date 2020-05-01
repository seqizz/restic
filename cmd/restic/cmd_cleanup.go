package main

import (
	"golang.org/x/sync/errgroup"

	"github.com/restic/restic/internal/crypto"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/pack"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"

	"github.com/spf13/cobra"
)

var cmdCleanup = &cobra.Command{
	Use:   "cleanup [flags]",
	Short: "Cleanup unused data",
	Long: `
The "cleanup" cleans up data in index and pack files
that is not referenced in any snapshot file.

When calling this command without flags, only packs
that are completely unused are deleted and index files are cleaned.
You can specify additional conditions to repack
packs that are only partly used or too small.
These packs will be downloaded and uploaded again which can be
quite time-consuming for remote repositories.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCleanup(cleanupOptions, globalOptions)
	},
}

// CleanupIndexOptions collects all options for the cleanup command.
type CleanupOptions struct {
	DryRun            bool
	DataUnusedPercent int8
	DataUnusedSpace   int64
	DataUsedSpace     int64
	TreeUnusedPercent int8
	TreeUnusedSpace   int64
	TreeUsedSpace     int64
	RepackMixed       bool
}

var cleanupOptions CleanupOptions

func init() {
	cmdRoot.AddCommand(cmdCleanup)

	f := cmdCleanup.Flags()
	f.BoolVarP(&cleanupOptions.DryRun, "dry-run", "n", false, "do not delete anything, just print what would be done")
	f.Int8Var(&cleanupOptions.DataUnusedPercent, "data-unused-percent", -1, "if > 0, repack data packs with >= given % unused space")
	f.Int64Var(&cleanupOptions.DataUnusedSpace, "data-unused-space", -1, "if > 0, repack data packs with >= given bytes unused space")
	f.Int64Var(&cleanupOptions.DataUsedSpace, "data-used-space", -1, "repack data packs with <= given bytes used space")
	f.Int8Var(&cleanupOptions.TreeUnusedPercent, "tree-unused-percent", -1, "if > 0, repack tree packs with >= given % unused space")
	f.Int64Var(&cleanupOptions.TreeUnusedSpace, "tree-unused-space", 1<<20, "if > 0, repack tree packs with >= given bytes unused space")
	f.Int64Var(&cleanupOptions.TreeUsedSpace, "tree-used-space", 1<<16, "repack tree packs with <= given bytes used space")
	f.BoolVar(&cleanupOptions.RepackMixed, "repack-mixed", true, "repack packs that have mixed blob types")
}

func runCleanup(opts CleanupOptions, gopts GlobalOptions) error {
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	lock, err := lockRepoExclusive(repo)
	defer unlockRepo(lock)
	if err != nil {
		return err
	}

	// get snapshot list
	Verbosef("get all snapshots\n")
	snapshots, err := restic.LoadAllSnapshots(gopts.ctx, repo)
	if err != nil {
		return err
	}

	Verbosef("load indexes\n")
	err = repo.LoadIndex(gopts.ctx)
	if err != nil {
		return err
	}

	usedBlobs, err := getUsedBlobs(gopts, repo, snapshots)
	if err != nil {
		return err
	}

	return Cleanup(opts, gopts, repo, usedBlobs)
}

func getUsedBlobs(gopts GlobalOptions, repo restic.Repository, snapshots []*restic.Snapshot) (restic.BlobSet, error) {
	ctx := gopts.ctx

	Verbosef("find data that is still in use for %d snapshots\n", len(snapshots))

	// usedBlobs will be used as used and seen Blobs
	usedBlobs := restic.NewBlobSet()

	bar := newProgressMax(!gopts.Quiet, uint64(len(snapshots)), "snapshots")
	bar.Start()
	for _, sn := range snapshots {
		debug.Log("process snapshot %v", sn.ID())

		err := restic.FindUsedBlobs(ctx, repo, *sn.Tree, usedBlobs, usedBlobs)
		if err != nil {
			if repo.Backend().IsNotExist(err) {
				return nil, errors.Fatal("unable to load a tree from the repo: " + err.Error())
			}

			return nil, err
		}

		debug.Log("processed snapshot %v", sn.ID())
		bar.Report(restic.Stat{Blobs: 1})
	}
	bar.Done()
	return usedBlobs, nil
}

type packInfo struct {
	length      uint
	usedBlobs   uint
	unusedBlobs uint
	tpe         restic.BlobType
}

const numLoadWorkers = 4

// Start with 4 bytes overhead per pack (len of header)
const headerLen = 4

func Cleanup(opts CleanupOptions, gopts GlobalOptions, repo restic.Repository, usedBlobs restic.BlobSet) error {

	ctx := gopts.ctx

	Verbosef("find packs in index and calculate used size...\n")

	indexPack := make(map[restic.ID]packInfo)
	keepBlobs := restic.NewBlobSet()

	for blob := range repo.Index().Each(ctx) {
		ip, ok := indexPack[blob.PackID]
		if !ok {
			ip = packInfo{length: headerLen, tpe: blob.Type, usedBlobs: 0, unusedBlobs: 0}
		}
		// mark mixed packs with "Invalid blob type"
		if ip.tpe != blob.Type {
			ip.tpe = restic.InvalidBlob
		}
		bh := restic.BlobHandle{ID: blob.ID, Type: blob.Type}
		if usedBlobs.Has(bh) {
			// overhead per blob is the entry Size of the header
			// + crypto overhead
			ip.length += blob.Length + pack.EntrySize + crypto.Extension
			ip.usedBlobs++

			usedBlobs.Delete(bh)
			keepBlobs.Insert(bh)
		} else {
			ip.unusedBlobs++
		}
		// update indexPack
		indexPack[blob.PackID] = ip
	}

	// Check if all used blobs has been found in index
	// TODO: maybe add flag --force ?
	if len(usedBlobs) > 0 {
		Warnf("The following blobs are missing in the index: %v", usedBlobs)
		return errors.New("Error: Index is not complete!")
	}

	Verbosef("repack and collect packs for deletion\n")
	removePacksFirst := restic.NewIDSet()
	removePacks := restic.NewIDSet()
	repackPacks := restic.NewIDSet()
	repackSmallPacks := restic.NewIDSet()
	removeBytes := uint64(0)
	repackBytes := uint64(0)
	repackFreeBytes := uint64(0)
	removeBlobs := uint(0)
	repackBlobs := uint(0)
	repackFreeBlobs := uint(0)

	err := repo.List(ctx, restic.DataFile, func(id restic.ID, packSize int64) error {
		p, ok := indexPack[id]
		usedSize := int64(p.length)
		unusedSize := packSize - usedSize
		unusedPercent := int8((unusedSize * 100) / packSize)

		switch {
		case !ok:
			// Pack was not referenced in index and is not used  => immediately remove!
			removePacksFirst.Insert(id)
			removeBytes += uint64(packSize)
		case usedSize == headerLen:
			// Pack was referenced in index but is longer used  => remove!
			removePacks.Insert(id)
			removeBytes += uint64(packSize)
			removeBlobs += p.unusedBlobs
		case opts.RepackMixed && p.tpe == restic.InvalidBlob,
			p.tpe == restic.DataBlob && opts.DataUnusedPercent > 0 && unusedPercent >= opts.DataUnusedPercent,
			p.tpe == restic.DataBlob && opts.DataUnusedSpace > 0 && unusedSize >= opts.DataUnusedSpace,
			p.tpe == restic.TreeBlob && opts.TreeUnusedPercent > 0 && unusedPercent >= opts.TreeUnusedPercent,
			p.tpe == restic.TreeBlob && opts.TreeUnusedSpace > 0 && unusedSize >= opts.TreeUnusedSpace:
			// pack has mixed blobtypes or fits conditions => repack!
			repackPacks.Insert(id)
			repackBytes += uint64(packSize)
			repackBlobs += p.usedBlobs
			repackFreeBytes += uint64(unusedSize)
			repackFreeBlobs += p.unusedBlobs
		case p.tpe == restic.DataBlob && usedSize <= opts.DataUsedSpace,
			p.tpe == restic.TreeBlob && usedSize <= opts.TreeUsedSpace:
			// pack is too small => repack!
			repackSmallPacks.Insert(id)
			repackBytes += uint64(packSize)
			repackBlobs += p.usedBlobs
			repackFreeBytes += uint64(unusedSize)
			repackFreeBlobs += p.unusedBlobs
		}
		return nil
	})
	if err != nil {
		return err
	}

	// If there is only one small Pack to repack, there is nothing to do...
	if len(repackSmallPacks) == 1 && len(repackPacks) == 0 {
		repackSmallPacks = restic.NewIDSet()
		repackBytes = 0
		repackBlobs = 0
		repackFreeBytes = 0
		repackFreeBlobs = 0
	}

	// The size informations are approximations because:
	// - it is not a priori known how many packs will result from the repacking
	// - rebuilding the index will also free some space
	Verbosef("found %d unreferenced packs\n", len(removePacksFirst))
	Verbosef("found %d no longer used packs with %d blobs\n", len(removePacks), removeBlobs)
	Verbosef("deleting unused packs will free about %s\n\n", formatBytes(removeBytes))

	Verbosef("found %d partly used packs + %d small packs = total %d packs with %d blobs for repacking with %s\n",
		len(repackPacks), len(repackSmallPacks), len(repackPacks)+len(repackSmallPacks), repackBlobs, formatBytes(repackBytes))
	Verbosef("repacking packs will delete %d blobs and free about %s\n", repackFreeBlobs, formatBytes(repackFreeBytes))

	Verbosef("cleanup-packs will totally delete %d blobs and free about %s\n", removeBlobs+repackFreeBlobs, formatBytes(removeBytes+repackFreeBytes))

	// from now on, treat small packs like all other to-repack packs
	repackPacks.Merge(repackSmallPacks)

	// unreferenced packs can be saveley deleted first
	if len(removePacksFirst) != 0 {
		Verbosef("deleting unreferenced pack files...\n")
		DeleteFiles(gopts, opts.DryRun, true, repo, removePacksFirst, restic.DataFile)
	}

	if len(repackPacks) > 0 {
		if !opts.DryRun {
			Verbosef("repacking packs...\n")
			bar := newProgressMax(!gopts.Quiet, uint64(repackBlobs), "blobs repacked")
			bar.Start()
			// TODO in Repack:  - Parallelize repacking
			//                  - Save full indexes during repacking
			//                  - Make use of blobs stored multiple times (e.g. if SHA doesn't match)
			obsolete, err := repository.Repack(ctx, repo, repackPacks, keepBlobs, bar)
			if err != nil {
				return err
			}
			bar.Done()
			// all packs-to-repack should now be obsolete!
			if !obsolete.Equals(repackPacks) {
				return errors.New("error: obsolete packs do not match packs-to-repack!")
			}
		} else {
			if !gopts.JSON && gopts.verbosity >= 2 {
				for id := range repackPacks {
					Verbosef("would have repacked pack %v.\n", id.Str())
				}
			}
		}

		// Also remove repacked pack at the end!
		removePacks.Merge(repackPacks)
	}

	var obsoleteIndexes restic.IDSet
	if len(removePacks) > 0 && !opts.DryRun {
		Verbosef("updating index files...\n")

		// TODO in RebuildIndex: - Save full indexes
		//						- Parallelize repacking
		//						- make it work with already saved indexes during Repack above
		obsoleteIndexes, err = (repo.Index()).(*repository.MasterIndex).RebuildIndex(ctx, removePacks)
		if err != nil {
			return err
		}
	}

	// save unfinished index
	err = repo.SaveIndex(ctx)
	if err != nil {
		return err
	}

	if len(obsoleteIndexes) != 0 {
		Verbosef("deleting obsolete index files...\n")
		err := DeleteFiles(gopts, opts.DryRun, false, repo, obsoleteIndexes, restic.IndexFile)
		if err != nil {
			return err
		}
	}

	if len(removePacks) != 0 {
		Verbosef("deleting obsolete pack files...\n")
		DeleteFiles(gopts, opts.DryRun, true, repo, removePacks, restic.DataFile)
	}

	Verbosef("done\n")
	return nil
}

const numDeleteWorkers = 8

func DeleteFiles(gopts GlobalOptions, dryrun bool, ignoreError bool, repo restic.Repository, fileList restic.IDSet, fileType restic.FileType) error {
	totalCount := len(fileList)
	fileHandles := make(chan restic.Handle)
	go func() {
		for id := range fileList {
			fileHandles <- restic.Handle{Type: fileType, Name: id.String()}
		}
		close(fileHandles)
	}()

	bar := newProgressMax(!gopts.Quiet, uint64(totalCount), "files deleted")
	wg, ctx := errgroup.WithContext(gopts.ctx)
	bar.Start()
	for i := 0; i < numDeleteWorkers; i++ {
		wg.Go(func() error {
			for h := range fileHandles {
				if !dryrun {
					err := repo.Backend().Remove(ctx, h)
					if err != nil {
						Warnf("unable to remove file %v from the repository\n", h.Name)
						if !ignoreError {
							return err
						}

					}
					if !gopts.JSON && gopts.verbosity >= 2 {
						Verbosef("%v was removed.\n", h.Name)
					}
				} else {
					if !gopts.JSON && gopts.verbosity >= 2 {
						Verbosef("would have removed %v.\n", h.Name)
					}
				}
				bar.Report(restic.Stat{Blobs: 1})
			}
			return nil
		})
	}
	err := wg.Wait()
	bar.Done()
	return err
}

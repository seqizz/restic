package main

import (
	"sort"

	"golang.org/x/sync/errgroup"

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
	DryRun           bool
	MaxUnusedPercent float32
	RepackSmall      bool
	RepackMixed      bool
	RepackDuplicates bool
	RepackTreesOnly  bool
	NoRebuildIndex   bool
}

var cleanupOptions CleanupOptions

func init() {
	cmdRoot.AddCommand(cmdCleanup)

	f := cmdCleanup.Flags()
	f.BoolVarP(&cleanupOptions.DryRun, "dry-run", "n", false, "do not modify the repository, just print what would be done")
	f.Float32Var(&cleanupOptions.MaxUnusedPercent, "max-unused-percent", 1.5, "tolerate given % of unused space in the repository")
	f.BoolVar(&cleanupOptions.RepackSmall, "repack-small", true, "always repack small pack files")
	f.BoolVar(&cleanupOptions.RepackMixed, "repack-mixed", true, "always repack packs that have mixed blob types")
	f.BoolVar(&cleanupOptions.RepackDuplicates, "repack-duplicates", true, "always repack packs that have duplicates of blobs")
	f.BoolVar(&cleanupOptions.RepackTreesOnly, "repack-trees-only", false, "only repack tree blobs")
	f.BoolVar(&cleanupOptions.NoRebuildIndex, "no-rebuild-index", false, "do not rebuild the index from packfiles after pruning")
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
	usedBlobs      uint
	unusedBlobs    uint
	duplicateBlobs uint
	usedSize       uint64
	unusedSize     uint64
	tpe            restic.BlobType
}

type packInfoWithID struct {
	ID restic.ID
	packInfo
}

const SmallPackSize = 1000000

func sizeInPack(blobLength uint) uint64 {
	return uint64(pack.PackedSizeOfBlob(blobLength))
}

func Cleanup(opts CleanupOptions, gopts GlobalOptions, repo restic.Repository, usedBlobs restic.BlobSet) error {

	ctx := gopts.ctx

	var stats struct {
		usedBlobs      uint
		duplicateBlobs uint
		unusedBlobs    uint
		removeBlobs    uint
		repackBlobs    uint
		usedSize       uint64
		duplicateSize  uint64
		unusedSize     uint64
		removeSize     uint64
		repackSize     uint64
	}

	Verbosef("find packs in index and calculate used size...\n")

	keepBlobs := restic.NewBlobSet()
	duplicateBlobs := restic.NewBlobSet()

	indexPack := make(map[restic.ID]packInfo)

	// run over all blobs in index to find out what blobs are duplicates
	for blob := range repo.Index().Each(ctx) {
		bh := blob.Handle()
		switch {
		case usedBlobs.Has(bh): // used blob
			stats.usedSize += sizeInPack(blob.Length)
			stats.usedBlobs++
			usedBlobs.Delete(bh)
			keepBlobs.Insert(bh)
		case keepBlobs.Has(bh): // duplicate blob
			stats.duplicateSize += sizeInPack(blob.Length)
			stats.duplicateBlobs++
			duplicateBlobs.Insert(bh)
		default: // unused blob
			stats.unusedSize += sizeInPack(blob.Length)
			stats.unusedBlobs++
		}
	}

	// Check if all used blobs has been found in index
	// TODO: maybe add flag --force ?
	if len(usedBlobs) != 0 {
		Warnf("There are following blobs are missing in the index, run restic check: %v\n", usedBlobs)
		return errors.New("Error: Index is not complete!")
	}

	// run over all blobs in index to generate packInfo
	for blob := range repo.Index().Each(ctx) {
		ip, ok := indexPack[blob.PackID]
		if !ok {
			ip = packInfo{tpe: blob.Type, usedSize: uint64(pack.HeaderSize())}
		}
		// mark mixed packs with "Invalid blob type"
		if ip.tpe != blob.Type {
			ip.tpe = restic.InvalidBlob
		}

		bh := blob.Handle()
		switch {
		case duplicateBlobs.Has(bh): // duplicate blob
			ip.usedSize += sizeInPack(blob.Length)
			ip.duplicateBlobs++
		case keepBlobs.Has(bh): // used blob, not duplicate
			ip.usedSize += sizeInPack(blob.Length)
			ip.usedBlobs++
		default: // unused blob
			ip.unusedSize += sizeInPack(blob.Length)
			ip.unusedBlobs++
		}
		// update indexPack
		indexPack[blob.PackID] = ip
	}

	Verbosef("collect packs for deletion and repacking...\n")
	removePacksFirst := restic.NewIDSet()
	removePacks := restic.NewIDSet()
	repackPacks := restic.NewIDSet()

	var repackCandidates []packInfoWithID

	err := repo.List(ctx, restic.DataFile, func(id restic.ID, packSize int64) error {
		p, ok := indexPack[id]
		if !ok {
			// Pack was not referenced in index and is not used  => immediately remove!
			removePacksFirst.Insert(id)
			stats.unusedSize += uint64(packSize)
			stats.removeSize += uint64(packSize)
			return nil
		}

		switch {
		case p.unusedBlobs == 0:
			// All blobs in pack are used => keep pack!

		case p.usedBlobs == 0 && p.duplicateBlobs == 0:
			// All blobs in pack are no longer used => remove pack!
			removePacks.Insert(id)
			stats.removeBlobs += p.unusedBlobs
			stats.removeSize += p.unusedSize

		case opts.RepackTreesOnly && p.tpe == restic.DataBlob:
			// if this is a data pack and --repack-trees-only is set => keep pack!

		case opts.RepackMixed && p.tpe == restic.InvalidBlob,
			opts.RepackSmall && packSize < SmallPackSize,
			opts.RepackDuplicates && p.duplicateBlobs > 0,
			opts.MaxUnusedPercent == 0.0:
			// repack if the according flag is set!
			repackPacks.Insert(id)
			stats.repackBlobs += p.unusedBlobs + p.duplicateBlobs + p.usedBlobs
			stats.repackSize += p.unusedSize + p.usedSize
			stats.removeBlobs += p.unusedBlobs
			stats.removeSize += p.unusedSize

		default:
			// all other packs are candidates for repacking
			repackCandidates = append(repackCandidates, packInfoWithID{ID: id, packInfo: p})
		}

		delete(indexPack, id)
		return nil
	})
	if err != nil {
		return err
	}

	if len(indexPack) != 0 {
		Warnf("There are packs in the index that are not present in the repository: %v\n", indexPack)
		return errors.New("Error: Packs from index missing in repo!")
	}

	if opts.RepackDuplicates || (opts.MaxUnusedPercent == 0.0) {
		stats.removeBlobs += stats.duplicateBlobs
		stats.removeSize += stats.duplicateSize
	}

	// If unused space is restricted, check and maybe find packs for repacking
	if opts.MaxUnusedPercent < 100.0 {
		maxUnusedSizeAfter := uint64(opts.MaxUnusedPercent / (100.0 - opts.MaxUnusedPercent) * float32(stats.usedSize))

		//sort repackCandidates such that packs with highest ratio unused/used space are picked first
		sort.Slice(repackCandidates, func(i, j int) bool {
			return repackCandidates[i].unusedSize*repackCandidates[j].usedSize >
				repackCandidates[j].unusedSize*repackCandidates[i].usedSize
		})

		for _, p := range repackCandidates {
			if stats.unusedSize < maxUnusedSizeAfter+stats.removeSize {
				break
			}
			repackPacks.Insert(p.ID)
			stats.repackBlobs += p.unusedBlobs + p.duplicateBlobs + p.usedBlobs
			stats.repackSize += p.unusedSize + p.usedSize
			stats.removeBlobs += p.unusedBlobs
			stats.removeSize += p.unusedSize
		}
	}

	Verbosef("used:       %8d blobs / %s\n", stats.usedBlobs, formatBytes(stats.usedSize))
	Verbosef("duplicates: %8d blobs / %s\n", stats.duplicateBlobs, formatBytes(stats.duplicateSize))
	Verbosef("unused:     %8d blobs / %s\n", stats.unusedBlobs, formatBytes(stats.unusedSize))
	totalSize := stats.usedSize + stats.duplicateSize + stats.unusedSize
	Verbosef("total:      %8d blobs / %s\n", stats.usedBlobs+stats.unusedBlobs+stats.duplicateBlobs,
		formatBytes(totalSize))
	Verbosef("unused size: %3.2f%% of total size\n\n", 100.0*float32(stats.unusedSize)/float32(totalSize))

	Verbosef("to repack:  %8d blobs / %s\n", stats.repackBlobs, formatBytes(stats.repackSize))
	Verbosef("to delete:  %8d blobs / %s\n", stats.removeBlobs, formatBytes(stats.removeSize))

	// unreferenced packs can be safely deleted first
	if len(removePacksFirst) != 0 {
		Verbosef("deleting unreferenced data files...\n")
		DeleteFiles(gopts, opts.DryRun, true, repo, removePacksFirst, restic.DataFile)
	}

	if len(repackPacks) != 0 {
		if !opts.DryRun {
			Verbosef("repacking packs...\n")
			bar := newProgressMax(!gopts.Quiet, uint64(stats.repackBlobs), "blobs repacked")
			bar.Start()
			// TODO in Repack:  - Parallelize repacking
			//                  - Save full indexes during repacking
			//                  - Make use of blobs stored multiple times (e.g. if SHA doesn't match)
			_, err := repository.Repack(ctx, repo, repackPacks, keepBlobs, bar)
			bar.Done()
			if err != nil {
				return err
			}
		} else {
			if !gopts.JSON {
				for id := range repackPacks {
					Verbosef("would have repacked pack %v.\n", id.Str())
				}
			}
		}

		// Also remove repacked packs
		removePacks.Merge(repackPacks)
	}

	if len(removePacks) != 0 && !opts.DryRun {
		Verbosef("updating index files...\n")

		if opts.NoRebuildIndex {
			// Call RebuildIndex: rebuilds the index from the already loaded in-memory index.
			// TODO in RebuildIndex: - Save full indexes
			//						- Parallelize repacking
			//						- make it work with already saved indexes during Repack above
			newIndex, obsoleteIndexes, err := (repo.Index()).(*repository.MasterIndex).RebuildIndex(removePacks)
			if err != nil {
				return err
			}

			_, err = repository.SaveIndex(ctx, repo, newIndex)
			if err != nil {
				return err
			}

			Verbosef("deleting obsolete index files...\n")
			err = DeleteFiles(gopts, opts.DryRun, false, repo, obsoleteIndexes, restic.IndexFile)
			if err != nil {
				return err
			}
		} else {
			// Call "restic rebuild-index": rebuild the index from pack files
			if err = rebuildIndex(ctx, repo, removePacks); err != nil {
				return err
			}
		}
	}

	if len(removePacks) != 0 {
		Verbosef("deleting obsolete data files...\n")
		DeleteFiles(gopts, opts.DryRun, true, repo, removePacks, restic.DataFile)
	}

	Verbosef("done.\n")
	return nil
}

const numDeleteWorkers = 8

// DeleteFiles deletes the given fileList of fileType in parallel
// if dryrun=true, it will just print out what would be deleted
// if ignoreError=true, it will print a warning if there was an error, else it will abort.
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
					if !gopts.JSON {
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

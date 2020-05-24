package main

import (
	"fmt"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/pack"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"

	"github.com/spf13/cobra"
)

var cmdPrune = &cobra.Command{
	Use:   "prune [flags]",
	Short: "Remove unneeded data from the repository",
	Long: `
The "prune" command checks the repository and removes data that is not
referenced and therefore not needed any more.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runPrune(pruneOptions, globalOptions)
	},
}

// PruneIndexOptions collects all options for the cleanup command.
type PruneOptions struct {
	DryRun           bool
	MaxUnusedPercent float32
	RepackSmall      bool
	RepackMixed      bool
	RepackDuplicates bool
	RepackTreesOnly  bool
}

var pruneOptions PruneOptions

func init() {
	cmdRoot.AddCommand(cmdPrune)
	f := cmdPrune.Flags()
	f.BoolVarP(&pruneOptions.DryRun, "dry-run", "n", false, "do not modify the repository, just print what would be done")
	addPruneFlags(cmdPrune)
}

func addPruneFlags(c *cobra.Command) {
	f := c.Flags()
	f.Float32Var(&pruneOptions.MaxUnusedPercent, "max-unused-percent", 1.5, "tolerate given % of unused space in the repository")
	f.BoolVar(&pruneOptions.RepackSmall, "repack-small", true, "always repack small data files")
	f.BoolVar(&pruneOptions.RepackMixed, "repack-mixed", true, "always repack data files that have mixed blob types")
	f.BoolVar(&pruneOptions.RepackDuplicates, "repack-duplicates", true, "always repack data files that have duplicates of blobs")
	f.BoolVar(&pruneOptions.RepackTreesOnly, "repack-trees-only", false, "only repack data files containing tree blobs")
}

func verifyPruneFlags(opts PruneOptions) error {
	if opts.MaxUnusedPercent < 0.0 || opts.MaxUnusedPercent > 100.0 {
		return errors.Fatalf("--max-unused-percent should be between 0 and 100. Given value: %f", opts.MaxUnusedPercent)
	}
	return nil
}

func shortenStatus(maxLength int, s string) string {
	if len(s) <= maxLength {
		return s
	}

	if maxLength < 3 {
		return s[:maxLength]
	}

	return s[:maxLength-3] + "..."
}

// newProgressMax returns a progress that counts blobs.
func newProgressMax(show bool, max uint64, description string) *restic.Progress {
	if !show {
		return nil
	}

	p := restic.NewProgress()

	p.OnUpdate = func(s restic.Stat, d time.Duration, ticker bool) {
		status := fmt.Sprintf("[%s] %s  %d / %d %s",
			formatDuration(d),
			formatPercent(s.Blobs, max),
			s.Blobs, max, description)

		if w := stdoutTerminalWidth(); w > 0 {
			status = shortenStatus(w, status)
		}

		PrintProgress("%s", status)
	}

	p.OnDone = func(s restic.Stat, d time.Duration, ticker bool) {
		fmt.Printf("\n")
	}

	return p
}

func runPrune(opts PruneOptions, gopts GlobalOptions) error {
	err := verifyPruneFlags(opts)
	if err != nil {
		return err
	}

	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	lock, err := lockRepoExclusive(repo)
	defer unlockRepo(lock)
	if err != nil {
		return err
	}

	return runPruneWithRepo(opts, gopts, repo)
}

func runPruneWithRepo(opts PruneOptions, gopts GlobalOptions, repo *repository.Repository) error {
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

	return Prune(opts, gopts, repo, usedBlobs)
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

func sizeInPack(blobLength uint) uint64 {
	return uint64(pack.PackedSizeOfBlob(blobLength))
}

func Prune(opts PruneOptions, gopts GlobalOptions, repo restic.Repository, usedBlobs restic.BlobSet) error {

	ctx := gopts.ctx

	var stats struct {
		blobs struct {
			used      uint
			duplicate uint
			unused    uint
			remove    uint
			repack    uint
			repackrm  uint
		}
		size struct {
			used      uint64
			duplicate uint64
			unused    uint64
			remove    uint64
			repack    uint64
			repackrm  uint64
			unref     uint64
		}
		packs struct {
			keepUsed       uint
			keepPartlyUsed uint
		}
	}

	Verbosef("find data files in index and calculate used size...\n")

	keepBlobs := restic.NewBlobSet()
	duplicateBlobs := restic.NewBlobSet()

	indexPack := make(map[restic.ID]packInfo)

	// run over all blobs in index to find out what blobs are duplicates
	for blob := range repo.Index().Each(ctx) {
		bh := blob.Handle()
		switch {
		case usedBlobs.Has(bh): // used blob
			stats.size.used += sizeInPack(blob.Length)
			stats.blobs.used++
			usedBlobs.Delete(bh)
			keepBlobs.Insert(bh)
		case keepBlobs.Has(bh): // duplicate blob
			stats.size.duplicate += sizeInPack(blob.Length)
			stats.blobs.duplicate++
			duplicateBlobs.Insert(bh)
		default: // unused blob
			stats.size.unused += sizeInPack(blob.Length)
			stats.blobs.unused++
		}
	}

	// Check if all used blobs has been found in index
	if len(usedBlobs) != 0 {
		Warnf("The following blobs are missing in the index, run restic check: %v\n", usedBlobs)
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

	Verbosef("collect data files for deletion and repacking...\n")
	removePacksFirst := restic.NewIDSet()
	removePacks := restic.NewIDSet()
	repackPacks := restic.NewIDSet()

	var repackCandidates []packInfoWithID

	repack := func(id restic.ID, p packInfo) {
		repackPacks.Insert(id)
		stats.blobs.repack += p.unusedBlobs + p.duplicateBlobs + p.usedBlobs
		stats.size.repack += p.unusedSize + p.usedSize
		stats.blobs.repackrm += p.unusedBlobs
		stats.size.repackrm += p.unusedSize
	}

	bar := newProgressMax(!gopts.Quiet, uint64(len(indexPack)), "data files processed")
	bar.Start()
	err := repo.List(ctx, restic.DataFile, func(id restic.ID, packSize int64) error {
		p, ok := indexPack[id]
		if !ok {
			// Pack was not referenced in index and is not used  => immediately remove!
			Verbosef("data file %s is not referenced in any index and not used -> will be removed.\n", id.Str())
			removePacksFirst.Insert(id)
			stats.size.unref += uint64(packSize)
			return nil
		}

		if p.unusedSize+p.usedSize != uint64(packSize) {
			Verbosef("data file %s: calculated size %d does not match real size %d difference: %d\n",
				id.Str(), p.unusedSize+p.usedSize, packSize, packSize-int64(p.unusedSize+p.usedSize))

		}

		switch {
		case p.unusedBlobs == 0:
			// All blobs in pack are used => keep pack!
			stats.packs.keepUsed++

		case p.usedBlobs == 0 && p.duplicateBlobs == 0:
			// All blobs in pack are no longer used => remove pack!
			removePacks.Insert(id)
			stats.blobs.remove += p.unusedBlobs
			stats.size.remove += p.unusedSize

		case opts.RepackTreesOnly && p.tpe != restic.TreeBlob:
			// if this is a data pack and --repack-trees-only is set => keep pack!
			stats.packs.keepPartlyUsed++

		case opts.RepackMixed && p.tpe == restic.InvalidBlob,
			opts.RepackSmall && packSize < repository.MinPackSize,
			opts.RepackDuplicates && p.duplicateBlobs > 0,
			opts.MaxUnusedPercent == 0.0:
			// repack if the according flag is set!
			repack(id, p)

		default:
			// all other packs are candidates for repacking
			repackCandidates = append(repackCandidates, packInfoWithID{ID: id, packInfo: p})
		}

		delete(indexPack, id)
		bar.Report(restic.Stat{Blobs: 1})
		return nil
	})
	bar.Done()
	if err != nil {
		return err
	}

	if len(indexPack) != 0 {
		Warnf("There are data files referenced in the index that are not present in the repository: %v\n", indexPack)
		return errors.New("Error: Data files from index missing in repo!")
	}

	// if all duplicates are repacked, print out correct statistics
	if opts.RepackDuplicates || (opts.MaxUnusedPercent == 0.0) {
		stats.blobs.repackrm += stats.blobs.duplicate
		stats.size.repackrm += stats.size.duplicate
	}

	// If unused space is restricted, check and find packs for repacking
	if opts.MaxUnusedPercent < 100.0 && opts.MaxUnusedPercent > 0.0 {
		maxUnusedSizeAfter := uint64(opts.MaxUnusedPercent / (100.0 - opts.MaxUnusedPercent) * float32(stats.size.used))

		//sort repackCandidates such that packs with highest ratio unused/used space are picked first
		sort.Slice(repackCandidates, func(i, j int) bool {
			return repackCandidates[i].unusedSize*repackCandidates[j].usedSize >
				repackCandidates[j].unusedSize*repackCandidates[i].usedSize
		})

		for i, p := range repackCandidates {
			if stats.size.unused < maxUnusedSizeAfter+stats.size.remove+stats.size.repackrm {
				stats.packs.keepPartlyUsed += uint(len(repackCandidates) - i)
				break
			}
			repack(p.ID, p.packInfo)
		}
	} else {
		stats.packs.keepPartlyUsed += uint(len(repackCandidates))
	}

	Verbosef("\nused:        %10d blobs / %s\n", stats.blobs.used, formatBytes(stats.size.used))
	Verbosef("duplicates:  %10d blobs / %s\n", stats.blobs.duplicate, formatBytes(stats.size.duplicate))
	Verbosef("unused:      %10d blobs / %s\n", stats.blobs.unused, formatBytes(stats.size.unused))
	Verbosef("unreferenced:                   %s\n", formatBytes(stats.size.unref))
	totalSize := stats.size.used + stats.size.duplicate + stats.size.unused + stats.size.unref
	Verbosef("total:       %10d blobs / %s\n", stats.blobs.used+stats.blobs.unused+stats.blobs.duplicate,
		formatBytes(totalSize))
	Verbosef("unused size: %s of total size\n\n", formatPercent(stats.size.unused, totalSize))

	Verbosef("to repack:   %10d blobs / %s\n", stats.blobs.repack, formatBytes(stats.size.repack))
	Verbosef("  -> prunes: %10d blobs / %s\n", stats.blobs.repackrm, formatBytes(stats.size.repackrm))
	Verbosef("to delete:   %10d blobs / %s\n", stats.blobs.remove, formatBytes(stats.size.remove))
	Verbosef("delete unreferenced:            %s\n", formatBytes(stats.size.unref))
	totalPruneSize := stats.size.remove + stats.size.repackrm + stats.size.unref
	Verbosef("total prune: %10d blobs / %s\n", stats.blobs.remove+stats.blobs.repackrm, formatBytes(totalPruneSize))
	Verbosef("unused size after prune: %s of total size\n\n",
		formatPercent(stats.size.unused-stats.size.remove-stats.size.repackrm, totalSize-totalPruneSize))

	Verbosef("total data files: %d / keep used: %d, keep partly used: %d, repack: %d, delete: %d, delete unreferenced: %d\n\n",
		int(stats.packs.keepUsed+stats.packs.keepPartlyUsed)+len(repackPacks)+len(removePacks)+len(removePacksFirst),
		stats.packs.keepUsed, stats.packs.keepPartlyUsed, len(repackPacks), len(removePacks), len(removePacksFirst))

	// delete obsolete index files (index files that have already been superseded)
	obsoleteIndexes := (repo.Index()).(*repository.MasterIndex).Obsolete()
	if len(obsoleteIndexes) != 0 {
		Verbosef("deleting unused index files...\n")
		DeleteFiles(gopts, opts.DryRun, true, repo, obsoleteIndexes, restic.IndexFile)
	}

	// unreferenced packs can be safely deleted first
	if len(removePacksFirst) != 0 {
		Verbosef("deleting unreferenced data files...\n")
		DeleteFiles(gopts, opts.DryRun, true, repo, removePacksFirst, restic.DataFile)
	}

	if len(repackPacks) != 0 {
		Verbosef("repacking data files...\n")
		if !opts.DryRun {
			bar := newProgressMax(!gopts.Quiet, uint64(len(repackPacks)), "data files repacked")
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
					Verbosef("would have repacked data file %v.\n", id.Str())
				}
			}
		}
		// Also remove repacked packs
		removePacks.Merge(repackPacks)
	}

	if len(removePacks) != 0 && !opts.DryRun {
		Verbosef("updating index files...\n")

		// Call RebuildIndex: rebuilds the index from the already loaded in-memory index.
		// TODO in RebuildIndex: - Parallelize repacking
		obsoleteIndexes, err := (repo.Index()).(*repository.MasterIndex).
			RebuildIndex(ctx, repo, removePacks)
		if err != nil {
			return err
		}

		Verbosef("deleting obsolete index files...\n")
		err = DeleteFiles(gopts, opts.DryRun, false, repo, obsoleteIndexes, restic.IndexFile)
		if err != nil {
			return err
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
				if dryrun {
					if !gopts.JSON {
						Verbosef("would have removed %v.\n", h.Name)
					}
					continue
				}
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
				bar.Report(restic.Stat{Blobs: 1})
			}
			return nil
		})
	}
	err := wg.Wait()
	bar.Done()
	return err
}

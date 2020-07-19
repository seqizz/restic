package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/pack"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"

	"github.com/spf13/cobra"
)

var errorIndexIncomplete = errors.New("index is not complete")
var errorPacksMissing = errors.New("packs from index missing in repo")

var cmdPrune = &cobra.Command{
	Use:   "prune [flags]",
	Short: "Remove unneeded data from the repository",
	Long: `
The "prune" command checks the repository and removes data that is not
referenced and therefore not needed any more.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
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
	MaxRepackCount   uint
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

const MaxUint = ^uint(0)

func addPruneFlags(c *cobra.Command) {
	f := c.Flags()
	f.Float32Var(&pruneOptions.MaxUnusedPercent, "max-unused-percent", 1.5, "tolerate given % of unused space in the repository")
	f.UintVar(&pruneOptions.MaxRepackCount, "max-repack-count", MaxUint, "maximum # of packs to repack")
	f.BoolVar(&pruneOptions.RepackSmall, "repack-small", false, "always repack small packs")
	f.BoolVar(&pruneOptions.RepackMixed, "repack-mixed", true, "always repack packs containing mixed blob types")
	f.BoolVar(&pruneOptions.RepackDuplicates, "repack-duplicates", true, "always repack packs containing duplicates of blobs")
	f.BoolVar(&pruneOptions.RepackTreesOnly, "repack-trees-only", false, "only repack packs which contain only tree blobs")
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

	return runPruneWithRepo(opts, gopts, repo, restic.NewIDSet())
}

func runPruneWithRepo(opts PruneOptions, gopts GlobalOptions, repo *repository.Repository, ignoreSnapshots restic.IDSet) error {
	// we do not need index updates while pruning!
	repo.DisableAutoIndexUpdate()

	Verbosef("get all snapshots\n")
	snapshots, err := restic.LoadAllSnapshots(gopts.ctx, repo, ignoreSnapshots)
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
			mixed      uint
			tree       uint
			data       uint
			small      uint
			used       uint
			unused     uint
			partlyUsed uint
			keep       uint
		}
	}

	Verbosef("find packs in index and calculate used size...\n")

	keepBlobs := restic.NewBlobSet()
	duplicateBlobs := restic.NewBlobSet()

	indexPack := make(map[restic.ID]packInfo)

	// run over all blobs in index to find out what blobs are duplicates
	for blob := range repo.Index().Each(ctx) {
		bh := blob.Handle()
		switch {
		case usedBlobs.Has(bh): // used blob, move to keepBlobs
			usedBlobs.Delete(bh)
			keepBlobs.Insert(bh)
		case keepBlobs.Has(bh): // duplicate blob
			duplicateBlobs.Insert(bh)
		}
	}

	// Check if all used blobs has been found in index
	if len(usedBlobs) != 0 {
		Warnf("The following blobs are missing in the index, run restic check: %v\n", usedBlobs)
		return errorIndexIncomplete
	}

	// run over all blobs in index to generate packInfo
	for blob := range repo.Index().Each(ctx) {
		ip, ok := indexPack[blob.PackID]
		if !ok {
			ip = packInfo{tpe: blob.Type, usedSize: pack.HeaderSize}
		}
		// mark mixed packs with "Invalid blob type"
		if ip.tpe != blob.Type {
			ip.tpe = restic.InvalidBlob
		}

		bh := blob.Handle()
		size := sizeInPack(blob.Length)
		switch {
		case duplicateBlobs.Has(bh): // duplicate blob
			ip.usedSize += size
			ip.duplicateBlobs++
			stats.size.duplicate += size
			stats.blobs.duplicate++
		case keepBlobs.Has(bh): // used blob, not duplicate
			ip.usedSize += size
			ip.usedBlobs++
			stats.size.used += size
			stats.blobs.used++
		default: // unused blob
			ip.unusedSize += size
			ip.unusedBlobs++
			stats.size.unused += size
			stats.blobs.unused++
		}
		// update indexPack
		indexPack[blob.PackID] = ip
	}

	Verbosef("collect packs for deletion and repacking...\n")
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

	// loop over all packs and decide what to do
	bar := newProgressMax(!gopts.Quiet, uint64(len(indexPack)), "packs processed")
	bar.Start()
	err := repo.List(ctx, restic.PackFile, func(id restic.ID, packSize int64) error {
		p, ok := indexPack[id]
		if !ok {
			// Pack was not referenced in index and is not used  => immediately remove!
			Verbosef("pack %s is not referenced in any index and not used -> will be removed.\n", id.Str())
			removePacksFirst.Insert(id)
			stats.size.unref += uint64(packSize)
			return nil
		}

		if p.unusedSize+p.usedSize != uint64(packSize) {
			Verbosef("pack %s: calculated size %d does not match real size %d difference: %d => treated as mixed pack\n",
				id.Str(), p.unusedSize+p.usedSize, packSize, packSize-int64(p.unusedSize+p.usedSize))
			p.tpe = restic.InvalidBlob
		}

		// statistics
		switch {
		case p.tpe == restic.InvalidBlob:
			stats.packs.mixed++
		case p.tpe == restic.TreeBlob:
			stats.packs.tree++
		default:
			stats.packs.data++
		}
		switch {
		case p.usedBlobs == 0 && p.duplicateBlobs == 0:
			stats.packs.unused++
		case packSize < repository.MinPackSize:
			stats.packs.small++
		case p.unusedBlobs == 0:
			stats.packs.used++
		default:
			stats.packs.partlyUsed++
		}

		// decide what to do
		switch {
		case p.usedBlobs == 0 && p.duplicateBlobs == 0:
			// All blobs in pack are no longer used => remove pack!
			removePacks.Insert(id)
			stats.blobs.remove += p.unusedBlobs
			stats.size.remove += p.unusedSize

		case opts.RepackTreesOnly && p.tpe != restic.TreeBlob,
			// if this is a data pack and --repack-trees-only is set => keep pack!
			uint(len(repackPacks)) >= opts.MaxRepackCount:
			// also keep if number of packs to repack is reached
			stats.packs.keep++

		case opts.RepackMixed && p.tpe == restic.InvalidBlob,
			opts.RepackSmall && packSize < repository.MinPackSize,
			opts.RepackDuplicates && p.duplicateBlobs > 0:
			// repack if the according flag is set!
			repack(id, p)

		case p.unusedBlobs == 0:
			// All blobs in pack are used => keep pack!
			stats.packs.keep++

		case opts.MaxUnusedPercent == 0.0:
			// repack all other packs if max-unusued-percent=0
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
		Warnf("There are packs referenced in the index that are not present in the repository: %v\n", indexPack)
		return errorPacksMissing
	}

	// if all duplicates are repacked, print out correct statistics
	if opts.RepackDuplicates || (opts.MaxUnusedPercent == 0.0) {
		stats.blobs.repackrm += stats.blobs.duplicate
		stats.size.repackrm += stats.size.duplicate
	}

	// If unused space is restricted, check and find packs for repacking
	if opts.MaxUnusedPercent < 100.0 {
		maxUnusedSizeAfter := uint64(opts.MaxUnusedPercent / (100.0 - opts.MaxUnusedPercent) * float32(stats.size.used))

		//sort repackCandidates such that packs with highest ratio unused/used space are picked first
		sort.Slice(repackCandidates, func(i, j int) bool {
			return repackCandidates[i].unusedSize*repackCandidates[j].usedSize >
				repackCandidates[j].unusedSize*repackCandidates[i].usedSize
		})

		for i, p := range repackCandidates {
			if stats.size.unused < maxUnusedSizeAfter+stats.size.remove+stats.size.repackrm || uint(len(repackPacks)) >= opts.MaxRepackCount {
				stats.packs.keep += uint(len(repackCandidates) - i)
				break
			}
			repack(p.ID, p.packInfo)
		}
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

	Verbosef("mixed packs:        %10d\n", stats.packs.mixed)
	Verbosef("tree packs:         %10d\n", stats.packs.tree)
	Verbosef("data packs:         %10d\n", stats.packs.data)
	Verbosef("unreferenced packs: %10d\n\n", len(removePacksFirst))
	Verbosef("total packs:        %10d\n\n",
		stats.packs.mixed+stats.packs.tree+stats.packs.data+uint(len(removePacksFirst)))

	Verbosef("small packs:        %10d\n", stats.packs.small)
	Verbosef("totally used packs: %10d\n", stats.packs.used)
	Verbosef("partly used packs:  %10d\n", stats.packs.partlyUsed)
	Verbosef("unused packs:       %10d\n\n", stats.packs.unused)

	Verbosef("to keep:   %10d packs\n", stats.packs.keep)
	Verbosef("to repack: %10d packs\n", len(repackPacks))
	Verbosef("to delete: %10d packs\n", len(removePacks))
	Verbosef("to delete: %10d unreferenced packs\n\n", len(removePacksFirst))

	if opts.DryRun {
		if !gopts.JSON && gopts.verbosity >= 2 {
			Printf("Would have removed the following unreferenced packs:\n%v\n\n", removePacksFirst)
			Printf("Would have repacked and removed the following packs:\n%v\n\n", repackPacks)
			Printf("Would have removed the following no longer used packs:\n%v\n\n", removePacks)
		}
		// Always quit here if DryRun was set!
		return nil
	}

	// unreferenced packs can be safely deleted first
	if len(removePacksFirst) != 0 {
		Verbosef("deleting unreferenced packs...\n")
		DeleteFiles(gopts, repo, removePacksFirst, restic.PackFile)
	}

	if len(repackPacks) != 0 {
		Verbosef("repacking packs...\n")
		bar := newProgressMax(!gopts.Quiet, uint64(len(repackPacks)), "packs repacked")
		bar.Start()
		// TODO in Repack:  - Parallelize repacking
		//                  - Make use of blobs stored multiple times (e.g. if SHA doesn't match)
		_, err := repository.Repack(ctx, repo, repackPacks, keepBlobs, bar)
		bar.Done()
		if err != nil {
			return err
		}
		// Also remove repacked packs
		removePacks.Merge(repackPacks)
	}

	if len(removePacks) != 0 {
		if err = rebuildIndex(ctx, repo, removePacks); err != nil {
			return err
		}

		Verbosef("remove %d old packs\n", len(removePacks))
		DeleteFiles(gopts, repo, removePacks, restic.PackFile)
	}

	Verbosef("done.\n")
	return nil
}

func getUsedBlobs(gopts GlobalOptions, repo restic.Repository, snapshots []*restic.Snapshot) (usedBlobs restic.BlobSet, err error) {
	ctx := gopts.ctx

	Verbosef("find data that is still in use for %d snapshots\n", len(snapshots))

	usedBlobs = restic.NewBlobSet()

	bar := newProgressMax(!gopts.Quiet, uint64(len(snapshots)), "snapshots")
	bar.Start()
	defer bar.Done()
	for _, sn := range snapshots {
		debug.Log("process snapshot %v", sn.ID())

		err = restic.FindUsedBlobs(ctx, repo, *sn.Tree, usedBlobs)
		if err != nil {
			if repo.Backend().IsNotExist(err) {
				return nil, errors.Fatal("unable to load a tree from the repo: " + err.Error())
			}

			return nil, err
		}

		debug.Log("processed snapshot %v", sn.ID())
		bar.Report(restic.Stat{Blobs: 1})
	}
	return usedBlobs, nil
}

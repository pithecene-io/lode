package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"strings"
	"time"
)

const (
	volumeManifestSchemaName    = "lode-volume-manifest"
	volumeManifestFormatVersion = "1.0.0"
)

// volume implements the Volume interface.
type volume struct {
	id          VolumeID
	store       Store
	totalLength int64
	checksum    Checksum

	// lastSnapshotID guards against stale-but-existing pointers after a
	// pointer write failure. See dataset.lastSnapshotID for rationale.
	lastSnapshotID VolumeSnapshotID
}

// NewVolume creates a volume with a fixed total length.
func NewVolume(id VolumeID, storeFactory StoreFactory, totalLength int64, opts ...VolumeOption) (Volume, error) {
	if storeFactory == nil {
		return nil, fmt.Errorf("lode: store factory must not be nil")
	}
	if id == "" {
		return nil, fmt.Errorf("lode: volume ID must not be empty")
	}
	if totalLength <= 0 {
		return nil, fmt.Errorf("lode: total length must be positive")
	}

	store, err := storeFactory()
	if err != nil {
		return nil, fmt.Errorf("lode: failed to create store: %w", err)
	}
	if store == nil {
		return nil, fmt.Errorf("lode: store factory returned nil store")
	}

	cfg := &volumeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	return &volume{
		id:          id,
		store:       store,
		totalLength: totalLength,
		checksum:    cfg.checksum,
	}, nil
}

// ID returns the volume's identifier.
func (v *volume) ID() VolumeID { return v.id }

// -----------------------------------------------------------------------------
// Path helpers (fixed layout per CONTRACT_VOLUME.md)
// -----------------------------------------------------------------------------

func volumeSnapshotsPrefix(id VolumeID) string {
	return path.Join("volumes", string(id), "snapshots") + "/"
}

func volumeManifestPath(id VolumeID, snapID VolumeSnapshotID) string {
	return path.Join("volumes", string(id), "snapshots", string(snapID), "manifest.json")
}

func volumeBlockPath(id VolumeID, offset, length int64) string {
	return path.Join("volumes", string(id), "data", fmt.Sprintf("%d-%d.bin", offset, length))
}

func volumeLatestPointerPath(id VolumeID) string {
	return path.Join("volumes", string(id), "latest")
}

// readLatestPointer reads the persistent latest-snapshot pointer file.
// Returns the snapshot ID or ErrNotFound if the pointer does not exist.
func (v *volume) readLatestPointer(ctx context.Context) (VolumeSnapshotID, error) {
	rc, err := v.store.Get(ctx, volumeLatestPointerPath(v.id))
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("lode: failed to read volume latest pointer: %w", err)
	}

	id := VolumeSnapshotID(strings.TrimSpace(string(data)))
	if id == "" {
		return "", ErrNotFound
	}
	return id, nil
}

// writeLatestPointer persists the snapshot ID as the latest pointer.
// Uses Delete+Put because Store.Put is no-overwrite.
func (v *volume) writeLatestPointer(ctx context.Context, id VolumeSnapshotID) error {
	pointerPath := volumeLatestPointerPath(v.id)
	_ = v.store.Delete(ctx, pointerPath) // ignore error; path may not exist
	return v.store.Put(ctx, pointerPath, strings.NewReader(string(id)))
}

// -----------------------------------------------------------------------------
// Write surface
// -----------------------------------------------------------------------------

// StageWriteAt writes data at an offset and returns a block handle.
// Staged data is not visible until Commit is called.
func (v *volume) StageWriteAt(ctx context.Context, offset int64, r io.Reader) (BlockRef, error) {
	if offset < 0 {
		return BlockRef{}, fmt.Errorf("lode: offset must be non-negative")
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return BlockRef{}, fmt.Errorf("lode: failed to read block data: %w", err)
	}

	length := int64(len(data))
	if length == 0 {
		return BlockRef{}, fmt.Errorf("lode: block data must not be empty")
	}
	if length > v.totalLength-offset {
		return BlockRef{}, fmt.Errorf("lode: block exceeds volume address space (offset=%d, length=%d, totalLength=%d)", offset, length, v.totalLength)
	}

	blockPath := volumeBlockPath(v.id, offset, length)

	var checksumStr string
	if v.checksum != nil {
		hasher := v.checksum.NewHasher()
		if _, err := hasher.Write(data); err != nil {
			return BlockRef{}, fmt.Errorf("lode: failed to compute checksum: %w", err)
		}
		checksumStr = hasher.Sum()
	}

	if err := v.store.Put(ctx, blockPath, bytes.NewReader(data)); err != nil {
		return BlockRef{}, fmt.Errorf("lode: failed to stage block: %w", err)
	}

	return BlockRef{
		Offset:   offset,
		Length:   length,
		Path:     blockPath,
		Checksum: checksumStr,
	}, nil
}

// Commit records the provided blocks into a new immutable snapshot.
func (v *volume) Commit(ctx context.Context, blocks []BlockRef, metadata Metadata) (*VolumeSnapshot, error) {
	if metadata == nil {
		metadata = Metadata{}
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("lode: commit must include at least one new block")
	}

	// Resolve parent via in-memory cache → pointer → scan.
	// In-memory cache is authoritative within this process and guards against
	// stale-but-existing pointers after a pointer write failure.
	var parentID VolumeSnapshotID
	var existingBlocks []BlockRef
	parentResolved := false

	if v.lastSnapshotID != "" {
		snap, snapErr := v.Snapshot(ctx, v.lastSnapshotID)
		if snapErr == nil {
			parentID = snap.ID
			existingBlocks = snap.Manifest.Blocks
			parentResolved = true
		} else if !errors.Is(snapErr, ErrNotFound) {
			return nil, fmt.Errorf("lode: failed to load snapshot from cache: %w", snapErr)
		}
		// Cache references nonexistent snapshot — fall through to pointer/scan.
	}
	if !parentResolved {
		latestID, pointerErr := v.readLatestPointer(ctx)
		if pointerErr == nil {
			snap, snapErr := v.Snapshot(ctx, latestID)
			if snapErr == nil {
				parentID = snap.ID
				existingBlocks = snap.Manifest.Blocks
				parentResolved = true
			} else if !errors.Is(snapErr, ErrNotFound) {
				return nil, fmt.Errorf("lode: failed to load snapshot from pointer: %w", snapErr)
			}
			// Pointer references nonexistent snapshot — fall through to scan.
		}
	}
	if !parentResolved {
		// No cache, no pointer, or stale: fall back to scan for backward compat.
		latest, scanErr := v.latestByScan(ctx)
		if scanErr != nil && !errors.Is(scanErr, ErrNoSnapshots) {
			return nil, fmt.Errorf("lode: failed to get latest snapshot: %w", scanErr)
		}
		if latest != nil {
			parentID = latest.ID
			existingBlocks = latest.Manifest.Blocks
		}
	}

	// Validate all new blocks have required fields, conform to fixed layout, and are within bounds.
	for _, b := range blocks {
		if b.Offset < 0 {
			return nil, fmt.Errorf("lode: block offset must be non-negative (offset=%d)", b.Offset)
		}
		if b.Length <= 0 {
			return nil, fmt.Errorf("lode: block length must be positive (length=%d)", b.Length)
		}
		if b.Length > v.totalLength-b.Offset {
			return nil, fmt.Errorf("lode: block exceeds volume address space (offset=%d, length=%d, totalLength=%d)", b.Offset, b.Length, v.totalLength)
		}
		expectedPath := volumeBlockPath(v.id, b.Offset, b.Length)
		if b.Path != expectedPath {
			return nil, fmt.Errorf("lode: block path %q does not match expected layout path %q", b.Path, expectedPath)
		}
	}

	// Verify at least one block is genuinely new (not already in the parent manifest).
	existingSet := make(map[string]struct{}, len(existingBlocks))
	for _, b := range existingBlocks {
		existingSet[b.Path] = struct{}{}
	}
	hasNew := false
	for _, b := range blocks {
		if _, found := existingSet[b.Path]; !found {
			hasNew = true
			break
		}
	}
	if !hasNew {
		return nil, fmt.Errorf("lode: commit must include at least one new block (all provided blocks already committed)")
	}

	// Build cumulative block set, sorted by offset for O(log B) lookups.
	// existingBlocks are sorted (guaranteed by prior commits). Sort only
	// the new blocks and merge for O(N + K log K) instead of O((N+K) log(N+K)).
	cumulativeBlocks := mergeBlocks(existingBlocks, blocks)

	// Validate no overlaps in the full cumulative set.
	if err := validateNoOverlaps(cumulativeBlocks); err != nil {
		return nil, err
	}

	snapshotID := VolumeSnapshotID(generateID())

	var checksumAlgorithm string
	if v.checksum != nil {
		checksumAlgorithm = v.checksum.Name()
	}

	manifest := &VolumeManifest{
		SchemaName:        volumeManifestSchemaName,
		FormatVersion:     volumeManifestFormatVersion,
		VolumeID:          v.id,
		SnapshotID:        snapshotID,
		CreatedAt:         time.Now().UTC(),
		Metadata:          metadata,
		TotalLength:       v.totalLength,
		Blocks:            cumulativeBlocks,
		ParentSnapshotID:  parentID,
		ChecksumAlgorithm: checksumAlgorithm,
	}

	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("lode: failed to marshal volume manifest: %w", err)
	}

	// Pointer must be written before manifest to prevent stale-but-existing
	// pointers on cold start. If this fails, no manifest is written and the
	// commit is aborted. A pointer referencing a not-yet-existing snapshot is
	// harmless (Exists check falls through to scan on the next cold start).
	if err := v.writeLatestPointer(ctx, snapshotID); err != nil {
		return nil, fmt.Errorf("lode: failed to update latest pointer: %w", err)
	}

	manifestPath := volumeManifestPath(v.id, snapshotID)
	if err := v.store.Put(ctx, manifestPath, bytes.NewReader(manifestData)); err != nil {
		return nil, fmt.Errorf("lode: failed to write volume manifest: %w", err)
	}
	v.lastSnapshotID = snapshotID

	return &VolumeSnapshot{
		ID:       snapshotID,
		Manifest: manifest,
	}, nil
}

// mergeBlocks merges two block slices into a single sorted-by-offset result.
// sorted must already be in ascending Offset order (from a prior commit).
// unsorted is sorted first, then the two are merged in O(N + K log K).
func mergeBlocks(sorted, unsorted []BlockRef) []BlockRef {
	if len(unsorted) == 0 {
		out := make([]BlockRef, len(sorted))
		copy(out, sorted)
		return out
	}

	// Sort new blocks (K is small — typically 1–10 blocks per commit).
	newBlocks := make([]BlockRef, len(unsorted))
	copy(newBlocks, unsorted)
	sort.Slice(newBlocks, func(i, j int) bool {
		return newBlocks[i].Offset < newBlocks[j].Offset
	})

	// Merge two sorted slices.
	result := make([]BlockRef, 0, len(sorted)+len(newBlocks))
	i, j := 0, 0
	for i < len(sorted) && j < len(newBlocks) {
		if sorted[i].Offset <= newBlocks[j].Offset {
			result = append(result, sorted[i])
			i++
		} else {
			result = append(result, newBlocks[j])
			j++
		}
	}
	result = append(result, sorted[i:]...)
	result = append(result, newBlocks[j:]...)
	return result
}

// validateNoOverlaps checks that blocks do not overlap in the cumulative set.
// Blocks are expected to be sorted by Offset (Commit sorts before calling).
// For backward compat, falls back to a defensive copy-and-sort if needed.
func validateNoOverlaps(blocks []BlockRef) error {
	if len(blocks) <= 1 {
		return nil
	}

	sorted := blocks
	if !sort.SliceIsSorted(sorted, func(i, j int) bool {
		return sorted[i].Offset < sorted[j].Offset
	}) {
		sorted = make([]BlockRef, len(blocks))
		copy(sorted, blocks)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Offset < sorted[j].Offset
		})
	}

	for i := 1; i < len(sorted); i++ {
		// Overflow-safe: equivalent to sorted[i-1].Offset + sorted[i-1].Length > sorted[i].Offset.
		// Safe because sorted is in ascending Offset order, so the subtraction is non-negative.
		if sorted[i-1].Length > sorted[i].Offset-sorted[i-1].Offset {
			return ErrOverlappingBlocks
		}
	}

	return nil
}

// -----------------------------------------------------------------------------
// Read surface
// -----------------------------------------------------------------------------

// ReadAt reads a fully committed range from a snapshot.
func (v *volume) ReadAt(ctx context.Context, snapshotID VolumeSnapshotID, offset, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, fmt.Errorf("lode: offset must be non-negative")
	}
	if length <= 0 {
		return nil, fmt.Errorf("lode: length must be positive")
	}
	if length > v.totalLength-offset {
		return nil, fmt.Errorf("lode: read exceeds volume address space (offset=%d, length=%d, totalLength=%d)", offset, length, v.totalLength)
	}

	snapshot, err := v.Snapshot(ctx, snapshotID)
	if err != nil {
		return nil, err
	}

	covering, err := findCoveringBlocks(snapshot.Manifest.Blocks, offset, length)
	if err != nil {
		return nil, err
	}

	if length > math.MaxInt {
		return nil, fmt.Errorf("lode: read length %d exceeds maximum allocation size", length)
	}
	result := make([]byte, int(length))
	for _, b := range covering {
		// Compute the intersection of [offset, offset+length) and [b.Offset, b.Offset+b.Length).
		readStart := max(offset, b.Offset)
		readEnd := min(offset+length, b.Offset+b.Length)
		readLen := readEnd - readStart
		storeOffset := readStart - b.Offset

		data, err := v.store.ReadRange(ctx, b.Path, storeOffset, readLen)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to read block at offset %d: %w", b.Offset, err)
		}
		if int64(len(data)) != readLen {
			return nil, fmt.Errorf("lode: block data truncated at offset %d (expected %d bytes, got %d)", b.Offset, readLen, len(data))
		}

		copy(result[readStart-offset:], data)
	}

	return result, nil
}

// findCoveringBlocks returns the blocks that fully cover [offset, offset+length).
// Returns ErrRangeMissing if any sub-range is not covered.
//
// Blocks are expected to be sorted by Offset (guaranteed for manifests written
// after the sorted-commit fix). Uses binary search for O(log B + R) where R is
// the number of covering blocks. For backward compat with unsorted manifests,
// falls back to a defensive copy-and-sort.
func findCoveringBlocks(blocks []BlockRef, offset, length int64) ([]BlockRef, error) {
	requestEnd := offset + length

	// Defensive: sort a copy if blocks are not already sorted (backward compat).
	sorted := blocks
	if !sort.SliceIsSorted(sorted, func(i, j int) bool {
		return sorted[i].Offset < sorted[j].Offset
	}) {
		sorted = make([]BlockRef, len(blocks))
		copy(sorted, blocks)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Offset < sorted[j].Offset
		})
	}

	// Binary search: find first block whose end exceeds the requested offset.
	start := sort.Search(len(sorted), func(i int) bool {
		return sorted[i].Offset+sorted[i].Length > offset
	})

	// Collect relevant blocks (walk forward from start).
	var relevant []BlockRef
	for i := start; i < len(sorted); i++ {
		if sorted[i].Offset >= requestEnd {
			break
		}
		relevant = append(relevant, sorted[i])
	}

	// Verify contiguous coverage from offset to requestEnd.
	cursor := offset
	for _, b := range relevant {
		if b.Offset > cursor {
			return nil, ErrRangeMissing
		}
		blockEnd := b.Offset + b.Length
		if blockEnd > cursor {
			cursor = blockEnd
		}
	}
	if cursor < requestEnd {
		return nil, ErrRangeMissing
	}

	return relevant, nil
}

// Latest returns the most recently committed snapshot.
func (v *volume) Latest(ctx context.Context) (*VolumeSnapshot, error) {
	// Pointer-first: O(1) via persistent latest file.
	id, err := v.readLatestPointer(ctx)
	if err == nil {
		snap, snapErr := v.Snapshot(ctx, id)
		if snapErr == nil {
			return snap, nil
		}
		// Pointer references a nonexistent snapshot — fall through to scan.
	}

	return v.latestByScan(ctx)
}

// latestByScan finds the latest snapshot via a single List + single Get.
// Snapshot IDs are nanosecond timestamps, so the lexicographically largest
// manifest path contains the latest snapshot.
func (v *volume) latestByScan(ctx context.Context) (*VolumeSnapshot, error) {
	prefix := volumeSnapshotsPrefix(v.id)
	paths, err := v.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("lode: failed to list volume snapshots: %w", err)
	}

	// Find the lexicographically largest snapshot ID from manifest paths.
	var latestID VolumeSnapshotID
	var latestPath string
	for _, p := range paths {
		if !strings.HasSuffix(p, "manifest.json") {
			continue
		}
		id := parseVolumeSnapshotID(p)
		if id == "" {
			continue
		}
		if string(id) > string(latestID) {
			latestID = id
			latestPath = p
		}
	}
	if latestID == "" {
		return nil, ErrNoSnapshots
	}

	snap, err := v.loadSnapshot(ctx, latestID, latestPath)
	if err != nil {
		return nil, err
	}

	// Self-heal: write the pointer so subsequent calls are O(1).
	_ = v.writeLatestPointer(ctx, latestID)

	return snap, nil
}

// Snapshots lists all committed snapshots sorted by creation time.
func (v *volume) Snapshots(ctx context.Context) ([]*VolumeSnapshot, error) {
	prefix := volumeSnapshotsPrefix(v.id)
	paths, err := v.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("lode: failed to list volume snapshots: %w", err)
	}

	var snapshots []*VolumeSnapshot
	for _, p := range paths {
		if !strings.HasSuffix(p, "manifest.json") {
			continue
		}
		snapID := parseVolumeSnapshotID(p)
		if snapID == "" {
			continue
		}
		snap, err := v.loadSnapshot(ctx, snapID, p)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snap)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Manifest.CreatedAt.Before(snapshots[j].Manifest.CreatedAt)
	})

	return snapshots, nil
}

// Snapshot retrieves a specific snapshot by ID.
func (v *volume) Snapshot(ctx context.Context, id VolumeSnapshotID) (*VolumeSnapshot, error) {
	manifestPath := volumeManifestPath(v.id, id)
	return v.loadSnapshot(ctx, id, manifestPath)
}

// loadSnapshot loads and validates a volume manifest from the given path.
func (v *volume) loadSnapshot(ctx context.Context, id VolumeSnapshotID, manifestPath string) (*VolumeSnapshot, error) {
	rc, err := v.store.Get(ctx, manifestPath)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("lode: failed to get volume manifest: %w", err)
	}
	defer func() { _ = rc.Close() }()

	var manifest VolumeManifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("lode: failed to decode volume manifest: %w", err)
	}

	if err := validateVolumeManifest(&manifest); err != nil {
		return nil, err
	}

	if manifest.VolumeID != v.id {
		return nil, fmt.Errorf("lode: manifest volume_id %q does not match volume %q", manifest.VolumeID, v.id)
	}
	if manifest.TotalLength != v.totalLength {
		return nil, fmt.Errorf("lode: manifest total_length %d does not match volume total_length %d", manifest.TotalLength, v.totalLength)
	}

	return &VolumeSnapshot{
		ID:       id,
		Manifest: &manifest,
	}, nil
}

// parseVolumeSnapshotID extracts the snapshot ID from a volume manifest path.
// Expected format: volumes/<id>/snapshots/<snapID>/manifest.json
func parseVolumeSnapshotID(p string) VolumeSnapshotID {
	parts := strings.Split(p, "/")
	// volumes/<id>/snapshots/<snapID>/manifest.json → 5 parts
	if len(parts) < 5 {
		return ""
	}
	return VolumeSnapshotID(parts[len(parts)-2])
}

// -----------------------------------------------------------------------------
// Volume manifest validation
// -----------------------------------------------------------------------------

// validateVolumeManifest checks that a volume manifest contains all required
// fields per CONTRACT_VOLUME.md.
func validateVolumeManifest(m *VolumeManifest) error {
	if m == nil {
		return &manifestValidationError{Field: "manifest", Message: "is nil"}
	}
	if m.SchemaName == "" {
		return &manifestValidationError{Field: "schema_name", Message: "is required"}
	}
	if m.FormatVersion == "" {
		return &manifestValidationError{Field: "format_version", Message: "is required"}
	}
	if m.VolumeID == "" {
		return &manifestValidationError{Field: "volume_id", Message: "is required"}
	}
	if m.SnapshotID == "" {
		return &manifestValidationError{Field: "snapshot_id", Message: "is required"}
	}
	if m.CreatedAt.IsZero() {
		return &manifestValidationError{Field: "created_at", Message: "is required"}
	}
	if m.Metadata == nil {
		return &manifestValidationError{Field: "metadata", Message: "must not be nil (use empty map for no metadata)"}
	}
	if m.Blocks == nil {
		return &manifestValidationError{Field: "blocks", Message: "must not be nil (use empty slice for no blocks)"}
	}
	if m.TotalLength <= 0 {
		return &manifestValidationError{Field: "total_length", Message: "must be positive"}
	}

	for i, b := range m.Blocks {
		if b.Offset < 0 {
			return &manifestValidationError{
				Field:   fmt.Sprintf("blocks[%d].offset", i),
				Message: "must be non-negative",
			}
		}
		if b.Length <= 0 {
			return &manifestValidationError{
				Field:   fmt.Sprintf("blocks[%d].length", i),
				Message: "must be positive",
			}
		}
		if b.Path == "" {
			return &manifestValidationError{
				Field:   fmt.Sprintf("blocks[%d].path", i),
				Message: "is required",
			}
		}
		if b.Length > m.TotalLength-b.Offset {
			return &manifestValidationError{
				Field:   fmt.Sprintf("blocks[%d]", i),
				Message: fmt.Sprintf("exceeds total_length (offset=%d, length=%d, total_length=%d)", b.Offset, b.Length, m.TotalLength),
			}
		}
	}

	if err := validateNoOverlaps(m.Blocks); err != nil {
		return &manifestValidationError{Field: "blocks", Message: "contain overlapping ranges"}
	}

	return nil
}

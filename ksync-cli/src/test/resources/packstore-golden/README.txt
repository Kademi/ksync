A pack store in the format ksync-go and ksync3 share, written by ksync-go.
Do not edit by hand. Regenerate with:
  go test ./hashsplit -run TestPackStoreGolden -update

objects/     pack-NNNNN.dat and index.dat, 8 packs (MaxPackSize 16 KiB, so
             objects spread across several and the pack id field is exercised)
manifest.txt one line per object, in index order: namespace (0 blob, 1 chunk
             fanout, 2 file fanout), hex hash, base64 of the exact stored bytes.
             A line with only two fields is an object with empty content.

Covers: "hello\n", whose blob, chunk fanout and file fanout share a hash; an
empty file; 614400 bytes of pseudo-random data (seed 0, see pseudoRandom in
pack_golden_test.go), whose first blob is cut at the 500001 byte cap and the
rest at content-defined boundaries; and one synthetic file fanout listing
nothing (hash eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee),
which no input produces but the format allows as bare digits.

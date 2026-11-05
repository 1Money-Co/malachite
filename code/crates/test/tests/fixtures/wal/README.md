# WAL compatibility fixtures

The two fixture directories qualify the production source and the newer development base independently:

- `bcac2b2d/` was generated at `bcac2b2dbd369a6c29b9151fcd0889b133d7221a`.
- `eb2405ba/` was generated at `eb2405ba78fc2d5c4465652364299f3217428a36`.

The resulting WAL files are byte-identical. Both are retained so each qualification source has explicit immutable
provenance. Their SHA-256 checksum is:

```text
a9e244eafdf9915e774a4bbf291cfc8a4608eac8c02052067ced483e8b3d7125  wal.log
```

For each source, `wal_compatibility_fixture.rs` was copied into a detached worktree and run from `code/`:

```console
cargo run -p informalsystems-malachitebft-test --example wal_compatibility_fixture -- \
  crates/test/tests/fixtures/wal/SOURCE SOURCE_COMMIT
```

The generator requires `shasum` to record the SHA-256 value. Do not regenerate these fixtures from a candidate
revision to resolve a compatibility failure; investigate the encoding change instead.

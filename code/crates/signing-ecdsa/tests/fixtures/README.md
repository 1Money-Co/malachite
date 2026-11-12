# Production ECDSA known-answer fixtures

These fixtures were generated from the production Malachite source at
`bcac2b2dbd369a6c29b9151fcd0889b133d7221a`, not from the current branch. They pin the raw key and signature
encodings plus the serde JSON representation for k256, p256, and p384.

The generator was copied into a detached worktree at that commit and run from `code/` with:

```console
cargo run -p informalsystems-malachitebft-signing-ecdsa \
  --example known_answer_fixtures --all-features
```

SHA-256 checksums:

```text
63648c843ab918fe3a390d3488d0e5dcb6c2873ddc260699e3246b5faf7af41f  k256.json
ef2f200d90c66d0e63252425acb77bada0b679ef37690587b7f02673513e1689  p256.json
601c5817abc4af427768cca4a13fa838a64b5308af5556f342ac1e68cb0a0ffb  p384.json
```

Do not regenerate these files from the current branch to resolve a compatibility failure. Such a failure is evidence
that the current implementation no longer reproduces the production encoding.

#!/usr/bin/env bash
set -euo pipefail

# Publish the `starling` crate to crates.io.

# Ensure we're in the crate root.
if [[ ! -f Cargo.toml ]]; then
  echo "Error: must be run from the crate root"
  exit 1
fi

# Check for uncommitted changes.
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Error: there are uncommitted changes. Commit before publishing."
  echo ""
  git status --short
  exit 1
fi

# Dry run first.
echo "==> Running dry-run for starling..."
cargo publish -p starling --dry-run

echo ""
read -p "Dry run passed. Publish to crates.io? [y/N] " confirm
if [[ "$confirm" != [yY] ]]; then
  echo "Aborted."
  exit 0
fi

echo ""
echo "==> Publishing starling..."
cargo publish -p starling

echo ""
echo "Done. Published starling v$(cargo metadata --no-deps --format-version 1 | grep -o '"version":"[^"]*"' | head -1 | cut -d'"' -f4)"

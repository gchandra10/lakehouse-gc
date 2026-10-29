#!/usr/bin/env bash
# uv.lock may be generated behind a PyPI mirror (e.g. a corporate proxy) that GitHub runners
# cannot reach. Mirrors serve the same files, so point the URLs at public PyPI; the hashes in
# the lock still verify every download.
set -euo pipefail
lock="${1:-uv.lock}"
perl -pi -e 's#https://[a-z0-9.-]*pypi-proxy[a-z0-9.-]*/simple/#https://pypi.org/simple#g; s#https://[a-z0-9.-]*pypi-proxy[a-z0-9.-]*/packages/#https://files.pythonhosted.org/packages/#g' "$lock"
if grep -q pypi-proxy "$lock"; then
  echo "::error::$lock still references a proxy after rewriting"
  exit 1
fi

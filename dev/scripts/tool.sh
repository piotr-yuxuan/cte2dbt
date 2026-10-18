#!/usr/bin/env bash
# Run a pinned validator with read-only source access and no container-engine socket.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [[ $# -eq 0 ]]; then
  printf 'Usage: tool.sh <actionlint|gitleaks> [arguments]\n' >&2
  exit 2
fi
tool=$1
shift
catalogue=dev/resources/ci/compose.yaml
# The image line of the tool's service block in the catalogue.
reference=$(awk -v service="  $tool:" '
  $0 == service { found = 1; next }
  /^  [^ ]/ { found = 0 }
  found && $1 == "image:" { print $2; exit }
' "$catalogue")
if [[ ! $reference =~ ^[[:alnum:]./_-]+:[[:alnum:]._-]+@sha256:[0-9a-f]{64}$ ]]; then
  printf 'tool.sh: %s needs an image:version@sha256:<digest> in %s\n' "$tool" "$catalogue" >&2
  exit 2
fi
engine=${CTE2DBT_CONTAINER_ENGINE:-docker}
name="cte2dbt-tool-$$"
mkdir -p dev/reports
common_dir=$(git rev-parse --path-format=absolute --git-common-dir)
cleanup() {
  "$engine" rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"$engine" run --rm --name "$name" --user 0:0 --entrypoint "$tool" \
  --read-only --cap-drop ALL \
  --security-opt no-new-privileges --tmpfs /tmp:rw,nosuid,nodev,size=2g \
  -v "$PWD:/repo:ro" -v "$PWD/dev/reports:/reports:rw" \
  -v "$common_dir:$common_dir:ro" -w /repo \
  -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0=/repo \
  "$reference" "$@"

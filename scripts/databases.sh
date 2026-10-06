#!/usr/bin/env bash
# Runs adya against a live database at each isolation level and checks every
# history against a ladder of consistency models.
#
#   scripts/databases.sh postgres postgres://user:pass@host/db
#   scripts/databases.sh mysql    mysql://user:pass@host/db
set -uo pipefail
target=$1
url=$2
adya=${ADYA:-target/release/adya}
mkdir -p out
summary=${GITHUB_STEP_SUMMARY:-/dev/stdout}
models="strict-serializable serializable snapshot-isolation repeatable-read read-committed read-uncommitted"

echo "### $target" >> "$summary"
echo "" >> "$summary"
echo "| isolation level | $(echo $models | sed 's/ / | /g') |" >> "$summary"
echo "|---|$(for m in $models; do printf -- '---|'; done)" >> "$summary"
for level in read-committed repeatable-read serializable; do
  for workload in list-append rw-register; do
    h=out/$target-$level-$workload.jsonl
    "$adya" run "$target" --url "$url" -i "$level" -m "$workload" -n 4000 -p 10 --keys 6 -o "$h" > /dev/null || true
    row="| $level ($workload) |"
    for m in $models; do
      "$adya" check -m "$workload" -c "$m" --json "$h" > "$h.$m.json"
      case $? in
        0) cell="valid" ;;
        1) cell=$(python3 -c "import json,sys; print(', '.join(json.load(open(sys.argv[1]))['anomaly_types']))" "$h.$m.json") ;;
        2) cell="unknown" ;;
        *) cell="error" ;;
      esac
      row="$row $cell |"
    done
    echo "$row" >> "$summary"
    echo "$row"
  done
done
echo "" >> "$summary"

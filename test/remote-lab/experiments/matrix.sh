#!/usr/bin/env bash
# The full tuning matrix. The lab must be up and seeded (see README.md).
set -u
cd "$(dirname "$0")/.." || exit 2
OUT=${1:?out dir}
mkdir -p "$OUT"
./experiments/sweep.py --profiles lan,same-city,abroad,abroad-noloss,hotel,cellular --configs base,bbr --writes --iperf --out "$OUT/cc.jsonl"
./experiments/sweep.py --profiles lan,same-city --configs ra32,ra128 --out "$OUT/ra.jsonl"
./experiments/sweep.py --profiles abroad,abroad-noloss,hotel,cellular --configs ra8,ra32,ra128,ra32-bbr,ra128-bbr,ra32-bbr-pf4 --out "$OUT/ra.jsonl"

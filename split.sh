#!/bin/bash
#
# Created By: Min Xu <xukmin@gmail.com>
#
# Script to split training data.

# Usage: split <filename> <percent-for-testing>
function split() {
  if (( $# != 2 )); then
    echo "Usage:   ${0} <filename> <percent-for-testing>"
    echo "Example: ${0} train_October_9_2012_clean.csv 1"
    return
  fi

  # local total=$(wc -l "${1}")
  local file="${1}"
  local percent="${2}"

  local lines="$(wc -l < "${file}")"
  local test_lines=$((lines * percent / 100))
  local train_lines=$((lines - test_lines))

  local base="$(basename "${file}")"
  base="${base%%.csv}"

  local train_file="/tmp/${base}_$((100 - percent)).csv"
  local test_file="/tmp/${base}_${percent}.csv"

  head -n "${train_lines}" "${file}" > "${train_file}"

  tail -n "${test_lines}" "${file}" > "${test_file}"

  hadoop fs -put -f "${train_file}" .
  hadoop fs -put -f "${test_file}" .
}

split "${@}"


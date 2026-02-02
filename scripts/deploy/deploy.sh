#!/bin/bash

# Usage: ./deploy.sh <config.yaml>
# YAML format:
# nodes:
#   - 192.168.96.84
#   - 192.168.96.85
#   - 192.168.96.86
#   - 192.168.96.87
#   - 192.168.96.88
# 
# gossip:
#   port_range:
#     start: 10010
#     end: 10020
#
# ports:
#   peer: 10000
#   client: 10005
#
# base_dir: /work/ctlplane


set -e

CFG_FILE="$1"
[ -f "$CFG_FILE" ] || exit 1

# Extract nodes (assumes simple YAML, no inline comments)
mapfile -t IPS < <(awk '
    $1 == "nodes:" {in_nodes=1; next}
    in_nodes && $1 ~ "-" {print $2; next}
    in_nodes && $1 !~ "-" {exit}
' "$CFG_FILE")

# Extract base_dir
BASE_DIR=$(awk -F: '$1 == "base_dir" {gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}' "$CFG_FILE")

[ "${#IPS[@]}" -gt 0 ] || exit 1
[ -n "$BASE_DIR" ] || exit 1

# Build pdsh hostlist
PDSH_HOSTS=$(IFS=','; echo "${IPS[*]}")

echo "WARNING: A fresh run will DELETE the existing database at ${BASE_DIR}/db!"
read -p "Do you want to continue with a fresh setup? [y/N]: " FRESH

if [[ "$FRESH" =~ ^[Yy]$ ]]; then
    rm -rf "${BASE_DIR}"
    mkdir -p "${BASE_DIR}/db"
    mkdir -p "${BASE_DIR}/ctl"
    mkdir -p "${BASE_DIR}/logs"

    rm -rf configs
    ./gen_raft_cfgs.sh "$CFG_FILE"

    cp -r configs "${BASE_DIR}/"
    cp -r lib "${BASE_DIR}/"
    cp -r libexec "${BASE_DIR}/"
    cp start_pumice.sh "${BASE_DIR}/"
fi

pdsh -w "${PDSH_HOSTS}" "${BASE_DIR}/start_pumice.sh"

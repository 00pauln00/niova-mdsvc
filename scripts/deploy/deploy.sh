#!/bin/bash
set -e

# Usage: sudo ./deploy.sh config.yaml

CFG_FILE="$(readlink -f "$1")"
[ -f "$CFG_FILE" ] || { echo "Config file not found"; exit 1; }

# ---------------- YAML PARSING ----------------

# nodes
mapfile -t IPS < <(awk '
    $1=="nodes:" {flag=1; next}
    flag && $1=="-" {print $2; next}
    flag && $1!~"-" {exit}
' "$CFG_FILE")

# gossip ports
START_GOSSIP_PORT=$(awk '/start:/ {print $2}' "$CFG_FILE")
END_GOSSIP_PORT=$(awk '/end:/ {print $2}' "$CFG_FILE")

# peer / client ports
PEER_PORT=$(awk '/peer:/ {print $2}' "$CFG_FILE")
CLIENT_PORT=$(awk '/client:/ {print $2}' "$CFG_FILE")

# base_dir
BASE_DIR=$(awk -F: '$1=="base_dir" {gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}' "$CFG_FILE")

[ "${#IPS[@]}" -gt 0 ] || { echo "No nodes found"; exit 1; }
[ -n "$BASE_DIR" ] || { echo "base_dir missing"; exit 1; }

PDSH_HOSTS=$(IFS=','; echo "${IPS[*]}")

# ---------------- FRESH SETUP ----------------

echo "WARNING: A fresh run will DELETE the existing database at ${BASE_DIR}/db!"
read -p "Do you want to continue with a fresh setup? [y/N]: " FRESH

if [[ "$FRESH" =~ ^[Yy]$ ]]; then
    rm -rf "${BASE_DIR}"
    mkdir -p "${BASE_DIR}/"{db,ctl,logs}
    cp "$CFG_FILE" "${BASE_DIR}/"

    rm -rf configs
    ./gen_raft_cfgs.sh "$CFG_FILE"

    cp -r configs "${BASE_DIR}/"
    cp ctlauth.yaml "${BASE_DIR}/"
    cp -r lib libexec start_pumice.sh "${BASE_DIR}/"
fi

pdsh -w "${PDSH_HOSTS}" "cd ${BASE_DIR} && ${BASE_DIR}/start_pumice.sh" "$CFG_FILE"

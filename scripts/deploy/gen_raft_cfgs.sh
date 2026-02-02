#!/bin/bash

# Usage: ./gen_raft_cfgs.sh <config.yaml>
#
# Expected YAML format:
# nodes:
#   - 10.0.0.84
#   - 10.0.0.85
# gossip:
#   port_range:
#     start: 10010
#     end: 10020
# ports:
#   peer: 10000
#   client: 10005
# base_dir: /work/ctlplane

set -e

CFG_FILE="$1"
[ -f "$CFG_FILE" ] || exit 1

mkdir -p configs
cd configs || exit 1

# ---- Parse YAML (simple awk-based parser, no dependencies) ----

# nodes
mapfile -t GOSSIP_IPS < <(awk '
    $1=="nodes:" {in=1; next}
    in && $1=="-" {print $2; next}
    in && $1!~"-" {exit}
' "$CFG_FILE")

# gossip ports
START_GOSSIP_PORT=$(awk '/start:/ {print $2}' "$CFG_FILE")
END_GOSSIP_PORT=$(awk '/end:/ {print $2}' "$CFG_FILE")

# peer / client ports
PEER_PORT=$(awk '/peer:/ {print $2}' "$CFG_FILE")
CLIENT_PORT=$(awk '/client:/ {print $2}' "$CFG_FILE")

# base directory
BASE_DIR=$(awk -F: '$1=="base_dir" {gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}' "$CFG_FILE")

[ "${#GOSSIP_IPS[@]}" -gt 0 ] || exit 1
[ -n "$BASE_DIR" ] || exit 1

# ---- Generate RAFT configs ----

RAFT_UUID=$(uuidgen)
RAFT_FILE="${RAFT_UUID}.raft"
echo "RAFT ${RAFT_UUID}" > "$RAFT_FILE"

for ip in "${GOSSIP_IPS[@]}"; do
    PEER_UUID=$(uuidgen)
    echo "PEER ${PEER_UUID}" >> "$RAFT_FILE"

    cat <<EOF > "${PEER_UUID}.peer"
RAFT         ${RAFT_UUID}
IPADDR       ${ip}
PORT         ${PEER_PORT}
CLIENT_PORT  ${CLIENT_PORT}
STORE        ${BASE_DIR}/db/${PEER_UUID}.raftdb
EOF
done

# ---- Gossip config ----

{
    echo "${GOSSIP_IPS[*]}"
    echo "${START_GOSSIP_PORT} ${END_GOSSIP_PORT}"
} > gossipNodes

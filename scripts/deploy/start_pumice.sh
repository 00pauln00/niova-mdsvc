#!/bin/bash
# start_pumice.sh
# Starts CTLPlane proxy and server.

# Usage: ./start_pumice.sh <config.yaml>

set -e

CFG_FILE="$1"
[ -f "$CFG_FILE" ] || exit 1

# ---- Parse YAML (simple, dependency-free) ----

BASE_DIR=$(awk -F: '$1=="base_dir" {gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}' "$CFG_FILE")

[ -n "$BASE_DIR" ] || exit 1

CONFIGS_DIR="${BASE_DIR}/configs"
LIB_DIR="${BASE_DIR}/lib"
LIBEXEC_DIR="${BASE_DIR}/libexec/niova"
CTL_DIR="${BASE_DIR}/ctl"
LOG_DIR="${BASE_DIR}/logs"

# ---- Environment ----

export NIOVA_LOCAL_CTL_SVC_DIR="${CONFIGS_DIR}"
export LD_LIBRARY_PATH="/lib:${LIB_DIR}"
export NIOVA_INOTIFY_BASE_PATH="${CTL_DIR}"
export NIOVA_APPLY_HANDLER_VERSION=0

# ---- Detect container IP ----

MY_IP=$(hostname -I | awk '{print $1}')

# ---- Raft UUID ----

RAFT_FILE=$(ls -1 "${CONFIGS_DIR}"/*.raft | head -n1)
[ -n "$RAFT_FILE" ] || exit 1
RAFT_UUID=$(basename "$RAFT_FILE" .raft)

# ---- Peer UUID (match IPADDR) ----

PEER_FILE=$(grep -il "IPADDR[[:space:]]\+${MY_IP}" "${CONFIGS_DIR}"/*.peer | head -n1)
[ -n "$PEER_FILE" ] || exit 1
PEER_UUID=$(basename "$PEER_FILE" .peer)

# ---- Start pmdb server ----

"${LIBEXEC_DIR}/CTLPlane_pmdbServer" \
    -r "${RAFT_UUID}" \
    -u "${PEER_UUID}" \
    -g "${CONFIGS_DIR}/gossipNodes" \
    -l "${LOG_DIR}/pmdb_server_${PEER_UUID}.log" \
    -p 1 \
    > "${LOG_DIR}/pmdb_server_${PEER_UUID}_stdouterr" 2>&1 &

sleep 5

# ---- Start proxy client ----

CUUID=$(uuidgen)

"${LIBEXEC_DIR}/CTLPlane_proxy" \
    -r "${RAFT_UUID}" \
    -u "${CUUID}" \
    -pa "${CONFIGS_DIR}/gossipNodes" \
    -n "Node_${CUUID}" \
    -l "${LOG_DIR}/pmdb_client_${CUUID}.log" \
    > "${LOG_DIR}/pmdb_client_${CUUID}_stdouterr" 2>&1

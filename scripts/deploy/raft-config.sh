#!/bin/bash

mkdir -p configs
cd configs || exit 1

# Generate RAFT UUID
START_IP_BASE="10.0.0."
PEER_PORT=10000
CLIENT_PORT=10005
START_GOSSIP_IP="192.168.96."
RAFT_UUID=$(uuidgen)
RAFT_FILE="${RAFT_UUID}.raft"
echo "RAFT ${RAFT_UUID}" > "$RAFT_FILE"

GOSSIP_IPS=()

for i in {0..4}; do
    PEER_UUID=$(uuidgen)
    echo "PEER ${PEER_UUID}" >> "$RAFT_FILE"
    PEER_FILE="${PEER_UUID}.peer"

    # Assign static IP based on Docker Compose network
    PEER_IP="${START_IP_BASE}$((84 + i))"  # 10.0.0.84 → node0, etc.
    GOSSIP_IP="${START_GOSSIP_IP}$((84 + i))" # 192.168.96.84 → node0, etc.
    GOSSIP_IPS+=("$GOSSIP_IP")

    cat <<EOF > "$PEER_FILE"
RAFT         ${RAFT_UUID}
IPADDR       ${PEER_IP}
PORT         ${PEER_PORT}
CLIENT_PORT  ${CLIENT_PORT}
STORE        /mnt/ctlplane/${PEER_UUID}.raftdb
EOF

    echo "Generated $PEER_FILE for $PEER_IP"
done

echo "Generated RAFT file: $RAFT_FILE"

# Create gossip config (IPs on first line, ports on second)
START_GOSSIP_PORT=10010
END_GOSSIP_PORT=$((START_GOSSIP_PORT + 10))

echo "GOSSIP CONFIG:"
echo "${GOSSIP_IPS[*]}" > gossipNodes
echo "$START_GOSSIP_PORT $END_GOSSIP_PORT" >> gossipNodes
cat gossipNodes

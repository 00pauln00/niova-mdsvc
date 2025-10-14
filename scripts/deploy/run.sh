#!/bin/bash
# pumice.sh
# Start the ctlplane node based on container IP using a single grep loop
export NIOVA_LOCAL_CTL_SVC_DIR="/work/ctlplane/configs"
export LD_LIBRARY_PATH="/lib:/work/ctlplane/lib"
export NIOVA_INOTIFY_BASE_PATH="/work/ctlplane/ctl"

# Detect container IP inside the Docker network
MY_IP=$(hostname -i | awk '{print $1}')
echo "Container IP detected as $MY_IP"

# Get Raft UUID (assume only one .raft file exists)
RAFT_FILE=$(ls -1 /work/ctlplane/configs/*.raft | head -n1)
RAFT_UUID=$(basename "$RAFT_FILE" .raft)

if [ -z "$RAFT_UUID" ]; then
    echo "ERROR: No .raft file found in configs/"
    exit 1
fi

# Find the .peer file containing the matching IPADDR in one grep pipeline
PEER_FILE=$(grep -il "IPADDR[[:space:]]\+$MY_IP" /work/ctlplane/configs/*.peer | head -n1)

if [ -z "$PEER_FILE" ]; then
    echo "ERROR: No .peer file found with IPADDR=$MY_IP"
    exit 1
fi

PEER_UUID=$(basename "$PEER_FILE" .peer)
echo "Starting node with PEER_UUID=$PEER_UUID and RAFT_UUID=$RAFT_UUID"

# Start pmdb server
/work/ctlplane/libexec/niova/CTLPlane_pmdbServer \
    -r "$RAFT_UUID" \
    -u "$PEER_UUID" \
    -g /work/ctlplane/configs/gossipNodes \
    -l "/work/ctlplane/logs/pmdb_server_${uuid}.log" \
    -p 1 > "/work/ctlplane/logs/pmdb_server_${PEER_UUID}_stdouterr" 2>&1 &

sleep 5

# Start proxy client
CUUID=$(uuidgen)
/work/ctlplane/libexec/niova/CTLPlane_proxy \
    -r "$RAFT_UUID" \
    -u "$CUUID" \
    -pa /work/ctlplane/configs/gossipNodes \
    -n "Node_${CUUID}" \
    -l "/work/ctlplane/logs/pmdb_client_${CUUID}.log" > "/work/ctlplane/logs/pmdb_client_${CUUID}_stdouterr" 2>&1

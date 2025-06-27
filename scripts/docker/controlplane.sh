#!/bin/bash

#Create configuration files
./raft-config.sh $1

RAFT_UUID=$(ls configs | awk -F. '/\.raft$/ { print $1 }')
mkdir logs


# Extract peer UUIDs from the configs directory
for file in ./configs/*.peer; do
    uuid="$(basename "$file" .peer)"
    
    # Run pmdb servers for each peer UUID
    ./libexec/niova/CTLPlane_pmdbServer \
        -g ./configs/gossipNodes \
        -r "${RAFT_UUID}" \
        -u "${uuid}" \
        -l "/controlplane/logs/pmdb_server_${uuid}.log" \
    	-p 0 > "/controlplane/logs/pmdb_server_${uuid}_stdouterr" 2>&1 &
done

sleep 5

CUUID="$(uuidgen)"

# Run the proxy
./libexec/niova/CTLPlane_proxy \
    -r "${RAFT_UUID}" \
    -u "${CUUID}" \
    -pa /controlplane/configs/gossipNodes \
    -n "Node_${CUUID}" \
    -l "/controlplane/logs/pmdb_client_${CUUID}.log" > "./logs/pmdb_client_${CUUID}_stdouterr" 2>&1

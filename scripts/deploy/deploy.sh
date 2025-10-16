#!/bin/bash

echo "WARNING: A fresh run will DELETE the existing database at /work/ctlplane/db!"
read -p "Do you want to continue with a fresh setup? [y/N]: " FRESH

if [[ "$FRESH" =~ ^[Yy]$ ]]; then
    echo "Performing fresh setup..."
    rm -rf /work/ctlplane
    mkdir -p /work/ctlplane/db
    mkdir -p /work/ctlplane/ctl
    mkdir -p /work/ctlplane/logs

    rm -rf configs
    ./raft-config.sh
    cp -r configs /work/ctlplane/
    cp -r lib /work/ctlplane/
    cp -r libexec /work/ctlplane/
    cp run.sh /work/ctlplane/
else
    echo "Skipping fresh setup."
fi

# Always run the remote script
pdsh -w 192.168.96.8[4-8] /work/ctlplane/run.sh

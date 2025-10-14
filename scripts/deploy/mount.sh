#!/bin/bash
# setup_ctlplane.sh
# Usage: ./setup_ctlplane.sh

NODES="192.168.96.8[4-8]"
DEVICE="/dev/nvme3n1"
MOUNTPOINT="/mnt/ctlplane"

echo "=== Checking device existence on all nodes ==="
pdsh -w $NODES "lsblk -f | grep -w $(basename $DEVICE)"

echo "=== Checking if device is already mounted ==="
pdsh -w $NODES "mount | grep -w $DEVICE" | while read -r line; do
    if [[ -n "$line" ]]; then
        echo "⚠️ Device $DEVICE is already mounted on a node:"
        echo "$line"
    fi
done

echo "=== Formatting device if not mounted (WARNING: will erase data) ==="
read -p "Are you sure you want to format $DEVICE on nodes where it is not mounted? [yes/NO]: " confirm
if [[ "$confirm" != "yes" ]]; then
    echo "Aborted."
    exit 1
fi

# Format only if not mounted
pdsh -w $NODES "mount | grep -qw $DEVICE || sudo mkfs.ext4 -F $DEVICE"

echo "=== Creating mount point ==="
pdsh -w $NODES "sudo mkdir -p $MOUNTPOINT"

echo "=== Mounting device if not already mounted ==="
pdsh -w $NODES "mount | grep -qw $DEVICE || sudo mount -o discard,defaults $DEVICE $MOUNTPOINT"

echo "=== Verifying mount ==="
pdsh -w $NODES "df -h $MOUNTPOINT"

#echo "=== Adding entry to /etc/fstab for persistence ==="
#pdsh -w $NODES "grep -q '$DEVICE' /etc/fstab || echo '$DEVICE $MOUNTPOINT ext4 defaults,discard 0 0' | sudo tee -a /etc/fstab"

echo "✅ All done! $DEVICE is formatted, mounted at $MOUNTPOINT (if not already), and fstab updated."

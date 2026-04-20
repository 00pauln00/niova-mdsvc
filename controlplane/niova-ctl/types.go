package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
)

// Type aliases to use libctlplanefuncs types
type Device = ctlplfl.Device
type DevicePartition = ctlplfl.DevicePartition
type PDU = ctlplfl.PDU
type Rack = ctlplfl.Rack
type Hypervisor = ctlplfl.Hypervisor
type Vdev = ctlplfl.Vdev

// DevicePartitionInfo holds information about a partition
type DevicePartitionInfo struct {
	Name string
	Size int64
}

// ParsePortRange parses a port range string like "8000-8100" and returns start and end ports
func ParsePortRange(portRange string) (int, int, error) {
	if portRange == "" {
		return 0, 0, fmt.Errorf("empty port range")
	}

	parts := strings.Split(portRange, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid port range format: %s", portRange)
	}

	start := 0
	end := 0
	var err error

	if start, err = strconv.Atoi(strings.TrimSpace(parts[0])); err != nil {
		return 0, 0, fmt.Errorf("invalid start port: %s", parts[0])
	}

	if end, err = strconv.Atoi(strings.TrimSpace(parts[1])); err != nil {
		return 0, 0, fmt.Errorf("invalid end port: %s", parts[1])
	}

	if start >= end {
		return 0, 0, fmt.Errorf("start port must be less than end port")
	}

	return start, end, nil
}

// AllocatePortPair allocates a contiguous pair of ports (serverPort=N, clientPort=N+1)
// from the given range, avoiding already allocated ports by querying existing NISDs
// from the control plane. Ports are isolated per hypervisor via FailureDomain[HV_IDX].
func AllocatePortPair(hypervisorUUID string, portRange string, userToken string, cpClient interface{}) (int, int, error) {
	startPort, endPort, err := ParsePortRange(portRange)
	if err != nil {
		return 0, 0, err
	}

	// Collect all allocated ports for this hypervisor
	allocatedPorts := make(map[int]bool)

	// Query existing NISDs from control plane to get allocated ports
	if cpClient != nil {
		if client, ok := cpClient.(interface {
			GetNisds(ctlplfl.GetReq) ([]ctlplfl.Nisd, error)
		}); ok {
			// Get all NISDs and filter by hypervisor UUID locally
			nisds, err := client.GetNisds(ctlplfl.GetReq{GetAll: true})
			if err == nil {
				for _, nisd := range nisds {
					if nisd.FailureDomain[ctlplfl.HV_IDX] == hypervisorUUID {
						allocatedPorts[int(nisd.PeerPort)] = true
						allocatedPorts[int(nisd.NetInfo[0].Port)] = true
					}
				}
			}
		}
	}

	for port := startPort; port < endPort-1; port += 2 {
		serverPort := port
		clientPort := port + 1

		if !allocatedPorts[serverPort] && !allocatedPorts[clientPort] {
			return clientPort, serverPort, nil
		}
	}

	return 0, 0, fmt.Errorf("no available port pairs in range %s", portRange)
}

// GetDeviceSize gets the actual size of a device in bytes
func GetDeviceSize(hv ctlplfl.Hypervisor, deviceName string) (int64, error) {
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	devicePath := fmt.Sprintf("/dev/%s", deviceName)

	checkCmd := fmt.Sprintf("test -b %s && echo 'exists' || echo 'not found'", devicePath)
	result, err := sshClient.RunCommand(checkCmd)
	if err != nil {
		return 0, fmt.Errorf("failed to check device %s: %v", devicePath, err)
	}
	if strings.TrimSpace(result) != "exists" {
		ip, err2 := hv.GetPrimaryIP()
		if err2 != nil {
			log.Error("GetDeviceSize():failed to fetch network info: ", err2)
		}
		return 0, fmt.Errorf("device %s not found on hypervisor %s", devicePath, ip)
	}

	sizeCmd := fmt.Sprintf("blockdev --getsize64 %s", devicePath)
	sizeResult, err := sshClient.RunCommand(sizeCmd)
	if err != nil {
		return 0, fmt.Errorf("failed to get size for device %s: %v", devicePath, err)
	}

	sizeBytes, err := strconv.ParseInt(strings.TrimSpace(sizeResult), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse device size: %v", err)
	}

	return sizeBytes, nil
}

// DeleteAllPartitionsFromDevice removes all partitions from a device
func DeleteAllPartitionsFromDevice(hv ctlplfl.Hypervisor, deviceName string) error {
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	devicePath := fmt.Sprintf("/dev/%s", deviceName)

	checkCmd := fmt.Sprintf("test -b %s && echo 'exists' || echo 'not found'", devicePath)
	result, err := sshClient.RunCommand(checkCmd)
	if err != nil {
		return fmt.Errorf("failed to check device %s: %v", devicePath, err)
	}
	if strings.TrimSpace(result) != "exists" {
		return fmt.Errorf("device %s not found on hypervisor %s", devicePath, hv.IPAddrs)
	}

	partCmd := fmt.Sprintf("parted -s %s mklabel gpt", devicePath)
	_, _ = sshClient.RunCommand(partCmd)
	log.Infof("Create partition table on %s (%s)", devicePath, partCmd)

	time.Sleep(2 * time.Second)

	return nil
}

// CreateMultipleEqualPartitions creates equal-sized partitions on a device
func CreateMultipleEqualPartitions(hv ctlplfl.Hypervisor, deviceName string, numPartitions int) error {
	if numPartitions <= 0 {
		return fmt.Errorf("number of partitions must be greater than 0")
	}

	_, err := GetDeviceSize(hv, deviceName)
	if err != nil {
		return fmt.Errorf("failed to get device size: %v", err)
	}

	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	devicePath := fmt.Sprintf("/dev/%s", deviceName)
	partitionPercentage := 100 / numPartitions

	for i := 0; i < numPartitions; i++ {
		startPercentage := i * partitionPercentage
		endPercentage := (i + 1) * partitionPercentage

		if i == numPartitions-1 {
			endPercentage = 100
		}

		partCmd := fmt.Sprintf("parted -s %s mkpart primary %d%% %d%%",
			devicePath, startPercentage, endPercentage)

		_, _ = sshClient.RunCommand(partCmd)
	}

	sshClient.RunCommand(fmt.Sprintf("partprobe %s", devicePath))
	sshClient.RunCommand("udevadm settle")
	time.Sleep(2 * time.Second)

	return nil
}

// GetDevicePartitionNames retrieves the actual partition names for a device using lsblk
func GetDevicePartitionNames(hv ctlplfl.Hypervisor, deviceName string) ([]string, error) {
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	cmd := fmt.Sprintf("lsblk -ln -o NAME /dev/%s", deviceName)
	output, err := sshClient.RunCommand(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition names for device %s: %v", deviceName, err)
	}

	lines := strings.Split(strings.TrimSpace(output), "\n")
	var partitionNames []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && line != deviceName {
			partitionNames = append(partitionNames, line)
		}
	}

	sort.Strings(partitionNames)
	return partitionNames, nil
}

// GetDevicePartitionInfo retrieves the actual partition names and sizes for a device using lsblk
func GetDevicePartitionInfo(hv ctlplfl.Hypervisor, deviceName string) ([]DevicePartitionInfo, error) {
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	byIdCmd := fmt.Sprintf("ls -la /dev/disk/by-id/ | grep '%s$' | head -1 | awk '{print $9}'", deviceName)
	byIdOutput, err := sshClient.RunCommand(byIdCmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get by-id name for device %s: %v", deviceName, err)
	}
	deviceByIdName := strings.TrimSpace(byIdOutput)
	if deviceByIdName == "" {
		return nil, fmt.Errorf("could not find by-id name for device %s", deviceName)
	}

	cmd := fmt.Sprintf("lsblk -ln -b -o NAME,SIZE /dev/%s", deviceName)
	output, err := sshClient.RunCommand(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition info for device %s: %v", deviceName, err)
	}

	lines := strings.Split(strings.TrimSpace(output), "\n")
	var partitionInfos []DevicePartitionInfo

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				partitionName := parts[0]
				if partitionName != deviceName {
					sizeStr := parts[1]
					size, err := strconv.ParseInt(sizeStr, 10, 64)
					if err != nil {
						log.Warnf("Failed to parse size for partition %s: %v", partitionName, err)
						size = 0
					}

					partitionByIdCmd := fmt.Sprintf("ls -la /dev/disk/by-id/ | grep '%s$' | head -1 | awk '{print $9}'", partitionName)
					partitionByIdOutput, err := sshClient.RunCommand(partitionByIdCmd)
					if err != nil {
						log.Warnf("Failed to get by-id name for partition %s: %v", partitionName, err)
						partitionInfos = append(partitionInfos, DevicePartitionInfo{
							Name: partitionName,
							Size: size,
						})
					} else {
						partitionByIdName := strings.TrimSpace(partitionByIdOutput)
						if partitionByIdName != "" {
							partitionInfos = append(partitionInfos, DevicePartitionInfo{
								Name: partitionByIdName,
								Size: size,
							})
						} else {
							partitionInfos = append(partitionInfos, DevicePartitionInfo{
								Name: partitionName,
								Size: size,
							})
						}
					}
				}
			}
		}
	}

	sort.Slice(partitionInfos, func(i, j int) bool {
		return partitionInfos[i].Name < partitionInfos[j].Name
	})
	return partitionInfos, nil
}

// getDeviceByIdName gets the /dev/disk/by-id/ name for a device
func getDeviceByIdName(hv ctlplfl.Hypervisor, deviceName string) (string, error) {
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return "", fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	byIdCmd := fmt.Sprintf("ls -la /dev/disk/by-id/ | grep '%s$' | head -1 | awk '{print $9}'", deviceName)
	byIdOutput, err := sshClient.RunCommand(byIdCmd)
	if err != nil {
		return "", fmt.Errorf("failed to get by-id name for device %s: %v", deviceName, err)
	}
	deviceByIdName := strings.TrimSpace(byIdOutput)
	if deviceByIdName == "" {
		return "", fmt.Errorf("could not find by-id name for device %s", deviceName)
	}

	return deviceByIdName, nil
}

// DeletePhysicalPartition removes an actual partition from the physical device
func DeletePhysicalPartition(hv ctlplfl.Hypervisor, deviceName string, partitionNumber int) error {
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	devicePath := fmt.Sprintf("/dev/%s", deviceName)

	checkCmd := fmt.Sprintf("test -b %s && echo 'exists' || echo 'not found'", devicePath)
	result, err := sshClient.RunCommand(checkCmd)
	if err != nil {
		return fmt.Errorf("failed to check device %s: %v", devicePath, err)
	}
	if strings.TrimSpace(result) != "exists" {
		return fmt.Errorf("device %s not found on hypervisor %s", devicePath, hv.IPAddrs)
	}

	checkPartCmd := fmt.Sprintf("parted -s %s print | grep '^ %d '", devicePath, partitionNumber)
	partResult, err := sshClient.RunCommand(checkPartCmd)
	if err != nil || strings.TrimSpace(partResult) == "" {
		return fmt.Errorf("partition %d not found on device %s", partitionNumber, devicePath)
	}

	deleteCmd := fmt.Sprintf("parted -s %s rm %d", devicePath, partitionNumber)
	_, err = sshClient.RunCommand(deleteCmd)
	if err != nil {
		return fmt.Errorf("failed to delete partition %d from %s: %v", partitionNumber, devicePath, err)
	}

	time.Sleep(2 * time.Second)

	verifyCmd := fmt.Sprintf("parted -s %s print | grep '^ %d ' | wc -l", devicePath, partitionNumber)
	verifyResult, err := sshClient.RunCommand(verifyCmd)
	if err != nil {
		return fmt.Errorf("failed to verify partition deletion: %v", err)
	}
	if strings.TrimSpace(verifyResult) != "0" {
		return fmt.Errorf("partition deletion verification failed for partition %d on %s", partitionNumber, devicePath)
	}

	return nil
}

// RemoveDevicePartition removes a physical NISD partition from a device via SSH
func RemoveDevicePartition(hv ctlplfl.Hypervisor, deviceName, partitionID string) error {
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}

	devicePath := fmt.Sprintf("/dev/%s", deviceName)

	// List partitions to find the matching one by index
	listCmd := fmt.Sprintf("parted -s %s print 2>/dev/null | grep -E '^ *[0-9]+'", devicePath)
	output, err := sshClient.RunCommand(listCmd)
	sshClient.Close()

	if err != nil {
		return fmt.Errorf("failed to list partitions on %s: %v", devicePath, err)
	}

	lines := strings.Split(strings.TrimSpace(output), "\n")
	for partIdx, line := range lines {
		if strings.Contains(line, partitionID) {
			return DeletePhysicalPartition(hv, deviceName, partIdx+1)
		}
	}

	// Partition may already be deleted
	log.Warnf("Partition %s not found on device %s, may already be removed", partitionID, deviceName)
	return nil
}

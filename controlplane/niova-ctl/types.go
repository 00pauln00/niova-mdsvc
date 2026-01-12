package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// Type aliases to use libctlplanefuncs types
type Device = ctlplfl.Device
type DevicePartition = ctlplfl.DevicePartition
type PDU = ctlplfl.PDU
type Rack = ctlplfl.Rack
type Hypervisor = ctlplfl.Hypervisor
type Vdev = ctlplfl.Vdev

type Config struct {
	PDUs  []ctlplfl.PDU  `json:"pdus"`
	Racks []ctlplfl.Rack `json:"racks"`
	// Legacy field for backward compatibility
	Hypervisors []ctlplfl.Hypervisor `json:"hypervisors,omitempty"`
}

// PDU Management
func (c *Config) AddPDU(pdu *ctlplfl.PDU) {
	// Generate UUID if not present
	if pdu.ID == "" {
		pdu.ID = uuid.New().String()
	}

	// Query control plane to get existing PDUs if client is available
	// This is handled in the UI layer where cpClient is available
	// Check if PDU exists by name
	for i, existing := range c.PDUs {
		if existing.Name == pdu.Name {
			// Preserve ID and racks when updating existing PDU
			pdu.ID = existing.ID
			if len(pdu.Racks) == 0 {
				pdu.Racks = existing.Racks
			}
			c.PDUs[i] = *pdu
			return
		}
	}
	// TODO here the new PDU gets added.
	c.PDUs = append(c.PDUs, *pdu)
}

func (c *Config) UpdatePDU(uuid string, pdu PDU) bool {
	for i, existing := range c.PDUs {
		if existing.ID == uuid {
			// Preserve ID and existing racks
			pdu.ID = uuid
			if len(pdu.Racks) == 0 {
				pdu.Racks = existing.Racks
			}
			c.PDUs[i] = pdu
			return true
		}
	}
	return false
}

func (c *Config) DeletePDU(uuid string) bool {
	for i, existing := range c.PDUs {
		if existing.ID == uuid {
			c.PDUs = append(c.PDUs[:i], c.PDUs[i+1:]...)
			return true
		}
	}
	return false
}

func (c *Config) GetPDU(uuid string) (*PDU, bool) {
	for i, pdu := range c.PDUs {
		if pdu.ID == uuid {
			return &c.PDUs[i], true
		}
	}
	return nil, false
}

// Rack Management
func (c *Config) AddRack(rack *ctlplfl.Rack) error {
	// Generate ID if not present
	if rack.ID == "" {
		rack.ID = uuid.New().String()
	}

	for i, pdu := range c.PDUs {
		if pdu.ID == rack.PDUID {
			// Check if rack exists by name within PDU
			for j, existing := range pdu.Racks {
				if existing.Name == rack.Name {
					// Preserve ID and hypervisors when updating
					rack.ID = existing.ID
					if len(rack.Hypervisors) == 0 {
						rack.Hypervisors = existing.Hypervisors
					}
					c.PDUs[i].Racks[j] = *rack
					return nil
				}
			}
			c.PDUs[i].Racks = append(c.PDUs[i].Racks, *rack)
			return nil
		}
	}
	return fmt.Errorf("PDU with ID %s not found", rack.PDUID)
}

func (c *Config) UpdateRack(rackUUID string, rack Rack) bool {
	for i, pdu := range c.PDUs {
		for j, existing := range pdu.Racks {
			if existing.ID == rackUUID {
				// Preserve UUID, PDU reference, and existing hypervisors
				rack.ID = rackUUID
				rack.PDUID = existing.PDUID
				if len(rack.Hypervisors) == 0 {
					rack.Hypervisors = existing.Hypervisors
				}
				c.PDUs[i].Racks[j] = rack
				return true
			}
		}
	}
	return false
}

func (c *Config) DeleteRack(rackUUID string) bool {
	for i, pdu := range c.PDUs {
		for j, rack := range pdu.Racks {
			if rack.ID == rackUUID {
				c.PDUs[i].Racks = append(c.PDUs[i].Racks[:j], c.PDUs[i].Racks[j+1:]...)
				return true
			}
		}
	}
	return false
}

func (c *Config) GetRack(rackUUID string) (*Rack, bool) {
	for i, pdu := range c.PDUs {
		for j, rack := range pdu.Racks {
			if rack.ID == rackUUID {
				return &c.PDUs[i].Racks[j], true
			}
		}
	}
	return nil, false
}

// Hypervisor Management (updated for hierarchy)
func (c *Config) AddHypervisor(rackUUID string, hv *ctlplfl.Hypervisor) error {
	// Generate UUID if not present
	if hv.ID == "" {
		hv.ID = uuid.New().String()
	}
	hv.RackID = rackUUID

	for i, pdu := range c.PDUs {
		for j, rack := range pdu.Racks {
			if rack.ID == rackUUID {
				// Check if hypervisor exists by IP address within rack
				for k, existing := range rack.Hypervisors {
					if ctlplfl.MatchIPs(existing.IPAddrs, hv.IPAddrs) {
						// Preserve UUID and devices when updating
						hv.ID = existing.ID
						if len(hv.Dev) == 0 {
							hv.Dev = existing.Dev
						}
						c.PDUs[i].Racks[j].Hypervisors[k] = *hv
						return nil
					}
				}
				c.PDUs[i].Racks[j].Hypervisors = append(c.PDUs[i].Racks[j].Hypervisors, *hv)
				return nil
			}
		}
	}
	return fmt.Errorf("rack with UUID %s not found", rackUUID)
}

// Legacy method for backward compatibility
func (c *Config) AddHypervisorLegacy(hv Hypervisor) {
	// Generate UUID if not present
	if hv.ID == "" {
		hv.ID = uuid.New().String()
	}

	// Check if hypervisor exists by IP address
	for i, existing := range c.Hypervisors {
		if ctlplfl.MatchIPs(existing.IPAddrs, hv.IPAddrs) {
			// Preserve UUID when updating existing hypervisor
			hv.ID = existing.ID
			c.Hypervisors[i] = hv
			return
		}
	}
	c.Hypervisors = append(c.Hypervisors, hv)
}

func (c *Config) UpdateHypervisor(hvUUID string, hv Hypervisor) bool {
	for i, pdu := range c.PDUs {
		for j, rack := range pdu.Racks {
			for k, existing := range rack.Hypervisors {
				if existing.ID == hvUUID {
					// Preserve UUID, rack reference, and existing devices
					hv.ID = hvUUID
					hv.RackID = existing.RackID
					if len(hv.Dev) == 0 {
						hv.Dev = existing.Dev
					}
					c.PDUs[i].Racks[j].Hypervisors[k] = hv
					return true
				}
			}
		}
	}
	// Check legacy hypervisors for backward compatibility
	for i, existing := range c.Hypervisors {
		if existing.ID == hvUUID {
			hv.ID = hvUUID
			c.Hypervisors[i] = hv
			return true
		}
	}
	return false
}

func (c *Config) DeleteHypervisor(hvUUID string) bool {
	for i, pdu := range c.PDUs {
		for j, rack := range pdu.Racks {
			for k, hv := range rack.Hypervisors {
				if hv.ID == hvUUID {
					c.PDUs[i].Racks[j].Hypervisors = append(c.PDUs[i].Racks[j].Hypervisors[:k], c.PDUs[i].Racks[j].Hypervisors[k+1:]...)
					return true
				}
			}
		}
	}
	// Check legacy hypervisors for backward compatibility
	for i, existing := range c.Hypervisors {
		if existing.ID == hvUUID {
			c.Hypervisors = append(c.Hypervisors[:i], c.Hypervisors[i+1:]...)
			return true
		}
	}
	return false
}

func (c *Config) GetHypervisor(hvUUID string) (*Hypervisor, bool) {
	for i, pdu := range c.PDUs {
		for j, rack := range pdu.Racks {
			for k, hv := range rack.Hypervisors {
				if hv.ID == hvUUID {
					return &c.PDUs[i].Racks[j].Hypervisors[k], true
				}
			}
		}
	}
	// Check legacy hypervisors for backward compatibility
	for i, hv := range c.Hypervisors {
		if hv.ID == hvUUID {
			return &c.Hypervisors[i], true
		}
	}
	return nil, false
}

func (c *Config) SaveToFile(filename string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

func LoadConfigFromFile(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return &Config{Hypervisors: []Hypervisor{}}, nil
	}
	if err != nil {
		return nil, err
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
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

// Helper methods for hierarchy navigation
func (c *Config) GetAllHypervisors() []Hypervisor {
	var hypervisors []Hypervisor
	for _, pdu := range c.PDUs {
		for _, rack := range pdu.Racks {
			hypervisors = append(hypervisors, rack.Hypervisors...)
		}
	}
	// Include legacy hypervisors for backward compatibility
	hypervisors = append(hypervisors, c.Hypervisors...)
	return hypervisors
}

func (c *Config) GetHypervisorFailureDomain(hvUUID string) string {
	for _, pdu := range c.PDUs {
		for _, rack := range pdu.Racks {
			for _, hv := range rack.Hypervisors {
				if hv.ID == hvUUID {
					return fmt.Sprintf("%s/%s/%s", pdu.Name, rack.Name, hv.Name)
				}
			}
		}
	}
	return ""
}

// AllocatePortPair allocates a client and server port pair from the given range,
// avoiding already allocated ports by querying existing NISDs from the control plane
func (c *Config) AllocatePortPair(hypervisorUUID string, portRange string, cpClient interface{}) (int, int, error) {
	startPort, endPort, err := ParsePortRange(portRange)
	if err != nil {
		return 0, 0, err
	}

	// Collect all allocated ports for this hypervisor
	allocatedPorts := make(map[int]bool)

	// Query existing NISDs from control plane to get allocated ports
	if cpClient != nil {
		// Type assert to get the actual client interface
		if client, ok := cpClient.(interface {
			GetNisds() ([]ctlplfl.Nisd, error)
		}); ok {
			// Create a request to get all NISDs
			// Get all NISDs and filter by hypervisor UUID locally
			nisds, err := client.GetNisds()
			if err == nil {
				// Process the NISDs to extract allocated ports for this hypervisor
				for _, nisd := range nisds {
					if nisd.FailureDomain[ctlplfl.FD_HV] == hypervisorUUID {
						// Mark the server port as allocated (NISD uses this)
						allocatedPorts[int(nisd.PeerPort)] = true
						// TODO maintain per - ip map and mark the ports in that map
						// Mark the client port as allocated
						allocatedPorts[int(nisd.NetInfo[0].Port)] = true
						// Mark client port + 1 as allocated (NISD uses this internally)
						allocatedPorts[int(nisd.NetInfo[0].Port)+1] = true
						// Mark the gap port after client port + 1 as allocated for spacing
						allocatedPorts[int(nisd.NetInfo[0].Port)+2] = true
					}
				}
			}
		}
	}

	// Find available port pair with proper spacing
	// Pattern: server_port, gap, client_port, gap for each NISD
	// NISD uses server_port and client_port + 1 internally
	// So allocation: server=X, client=X+2 → NISD uses X and X+3, next NISD starts at X+4
	for port := startPort; port < endPort-3; port += 4 {
		serverPort := port     // Server port for NISD (NISD uses this)
		gapPort1 := port + 1   // Gap port (reserved for spacing)
		clientPort := port + 2 // Client port for NISD
		gapPort2 := port + 3   // Gap port (NISD uses client_port + 1 = this port)

		if !allocatedPorts[serverPort] && !allocatedPorts[gapPort1] &&
			!allocatedPorts[clientPort] && !allocatedPorts[gapPort2] {
			return clientPort, serverPort, nil
		}
	}

	return 0, 0, fmt.Errorf("no available port pairs in range %s", portRange)
}

// InitializeDevice initializes a device with UUID, ports, and other metadata
func (c *Config) InitializeDevice(hypervisorUUID, deviceName, failureDomain string) error {
	// Check hierarchical structure
	for i, pdu := range c.PDUs {
		for j, rack := range pdu.Racks {
			for k, hv := range rack.Hypervisors {
				if hv.ID == hypervisorUUID {
					for devIndex, dev := range hv.Dev {
						if dev.Name == deviceName && dev.State != ctlplfl.INITIALIZED {

							/*
								// Allocate ports
								_, _, err := c.AllocatePortPair(hypervisorUUID, hv.PortRange)
								if err != nil {
									return fmt.Errorf("failed to allocate ports: %v", err)
								}
							*/

							// Build hierarchical failure domain
							hierarchicalFailureDomain := fmt.Sprintf("%s/%s/%s/%s", pdu.Name, rack.Name, hv.Name, failureDomain)

							// Update device
							c.PDUs[i].Racks[j].Hypervisors[k].Dev[devIndex] = Device{
								ID:            dev.ID,
								Name:          dev.Name,
								Size:          dev.Size,
								HypervisorID:  hypervisorUUID,
								DevicePath:    fmt.Sprintf("/dev/%s", dev.Name),
								FailureDomain: hierarchicalFailureDomain,
								State:         ctlplfl.INITIALIZED,
							}

							return nil
						}
					}
					return fmt.Errorf("device %s not found or already initialized", deviceName)
				}
			}
		}
	}

	// Check legacy hypervisors for backward compatibility
	for hvIndex, hv := range c.Hypervisors {
		if hv.ID == hypervisorUUID {
			for devIndex, dev := range hv.Dev {
				if dev.Name == deviceName && dev.State != ctlplfl.INITIALIZED {

					/*
						// Allocate ports
						_, _, err := c.AllocatePortPair(hypervisorUUID, hv.PortRange)
						if err != nil {
							return fmt.Errorf("failed to allocate ports: %v", err)
						}
					*/

					// Update device
					c.Hypervisors[hvIndex].Dev[devIndex] = Device{
						ID:            dev.ID,
						Name:          dev.Name,
						Size:          dev.Size,
						HypervisorID:  hypervisorUUID,
						DevicePath:    fmt.Sprintf("/dev/%s", dev.Name),
						FailureDomain: failureDomain,
						State:         ctlplfl.INITIALIZED,
					}

					return nil
				}
			}
			return fmt.Errorf("device %s not found or already initialized", deviceName)
		}
	}
	return fmt.Errorf("hypervisor with UUID %s not found", hypervisorUUID)
}

// GetDeviceSize gets the actual size of a device in bytes
func (c *Config) GetDeviceSize(hvUUID, deviceName string) (int64, error) {
	hv, found := c.GetHypervisor(hvUUID)
	if !found {
		return 0, fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Create SSH connection to hypervisor
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	// Get the full device path
	devicePath := fmt.Sprintf("/dev/%s", deviceName)

	// Check if device exists
	checkCmd := fmt.Sprintf("test -b %s && echo 'exists' || echo 'not found'", devicePath)
	result, err := sshClient.RunCommand(checkCmd)
	if err != nil {
		return 0, fmt.Errorf("failed to check device %s: %v", devicePath, err)
	}
	if strings.TrimSpace(result) != "exists" {
		ip, err := hv.GetPrimaryIP()
		if err != nil {
			log.Error("GetDeviceSize():failed to fetch network info: ", err)
		}

		return 0, fmt.Errorf("device %s not found on hypervisor %s", devicePath, ip)
	}

	// Get device size in bytes using blockdev
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
func (c *Config) DeleteAllPartitionsFromDevice(hvUUID, deviceName string) error {
	hv, found := c.GetHypervisor(hvUUID)
	if !found {
		return fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Create SSH connection to hypervisor
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	// Get the full device path
	devicePath := fmt.Sprintf("/dev/%s", deviceName)

	// Check if device exists
	checkCmd := fmt.Sprintf("test -b %s && echo 'exists' || echo 'not found'", devicePath)
	result, err := sshClient.RunCommand(checkCmd)
	if err != nil {
		return fmt.Errorf("failed to check device %s: %v", devicePath, err)
	}
	if strings.TrimSpace(result) != "exists" {
		return fmt.Errorf("device %s not found on hypervisor %s", devicePath, hv.IPAddrs)
	}

	// Create new GPT partition table (this removes all existing partitions)
	partCmd := fmt.Sprintf("parted -s %s mklabel gpt", devicePath)
	_, _ = sshClient.RunCommand(partCmd)
	log.Info("Create partition table on %s (%s): %v", devicePath, partCmd)
	//if err != nil {
	//	log.Info("failed to delete partition table on %s (%s): %v", devicePath, deleteCmd, err)
	//	return fmt.Errorf("failed to delete partition table on %s (%s): %v", devicePath, deleteCmd, err)
	//}

	// Wait for kernel to update
	time.Sleep(2 * time.Second)

	return nil
}

// CreateMultipleEqualPartitions creates equal-sized partitions on a device
func (c *Config) CreateMultipleEqualPartitions(hvUUID, deviceName string, numPartitions int) error {
	if numPartitions <= 0 {
		return fmt.Errorf("number of partitions must be greater than 0")
	}

	// Get device size
	_, err := c.GetDeviceSize(hvUUID, deviceName)
	if err != nil {
		return fmt.Errorf("failed to get device size: %v", err)
	}

	// FIXME
	// Delete existing partition table
	//err = c.DeleteAllPartitionsFromDevice(hvUUID, deviceName)
	//if err != nil {
	//	return fmt.Errorf("failed to delete existing partitions: %v", err)
	//}

	hv, found := c.GetHypervisor(hvUUID)
	if !found {
		return fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Create SSH connection to hypervisor
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	// Get the full device path
	devicePath := fmt.Sprintf("/dev/%s", deviceName)

	// Calculate partition percentages
	partitionPercentage := 100 / numPartitions

	// Create partitions using percentage-based approach
	for i := 0; i < numPartitions; i++ {
		startPercentage := i * partitionPercentage
		endPercentage := (i + 1) * partitionPercentage

		// For the last partition, use exactly 100% to avoid rounding issues
		if i == numPartitions-1 {
			endPercentage = 100
		}

		partCmd := fmt.Sprintf("parted -s %s mkpart primary %d%% %d%%",
			devicePath, startPercentage, endPercentage)

		_, _ = sshClient.RunCommand(partCmd)
		//FIXME the partCmd returns error but partition gets created successfully, need fix.
		//if err != nil {
		//	return fmt.Errorf("failed to create partition %d on %s: %v", i+1, devicePath, err)
		//}
	}

	// Wait for partitions to be recognized by kernel
	time.Sleep(3 * time.Second)

	return nil
}

// GetDevicePartitionNames retrieves the actual partition names for a device using lsblk
func (c *Config) GetDevicePartitionNames(hvUUID, deviceName string) ([]string, error) {
	hv, found := c.GetHypervisor(hvUUID)
	if !found {
		return nil, fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Create SSH connection to hypervisor
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	// Use lsblk to get partition names for the specific device
	// The command gets all partitions for the device, sorted by partition number
	cmd := fmt.Sprintf("lsblk -ln -o NAME /dev/%s | grep -E '^%s' | tail -n +2 | sort", deviceName, deviceName)
	output, err := sshClient.RunCommand(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition names for device %s: %v", deviceName, err)
	}

	// Parse the output to get partition names
	lines := strings.Split(strings.TrimSpace(output), "\n")
	var partitionNames []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && line != deviceName {
			partitionNames = append(partitionNames, line)
		}
	}

	return partitionNames, nil
}

// DevicePartitionInfo holds information about a partition
type DevicePartitionInfo struct {
	Name string
	Size int64
}

// GetDevicePartitionInfo retrieves the actual partition names and sizes for a device using lsblk
func (c *Config) GetDevicePartitionInfo(hvUUID, deviceName string) ([]DevicePartitionInfo, error) {
	hv, found := c.GetHypervisor(hvUUID)
	if !found {
		return nil, fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Create SSH connection to hypervisor
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	// First, find the by-id name for the device
	byIdCmd := fmt.Sprintf("ls -la /dev/disk/by-id/ | grep '%s$' | head -1 | awk '{print $9}'", deviceName)
	byIdOutput, err := sshClient.RunCommand(byIdCmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get by-id name for device %s: %v", deviceName, err)
	}
	deviceByIdName := strings.TrimSpace(byIdOutput)
	if deviceByIdName == "" {
		return nil, fmt.Errorf("could not find by-id name for device %s", deviceName)
	}

	// Get partition information using lsblk
	cmd := fmt.Sprintf("lsblk -ln -b -o NAME,SIZE /dev/%s | grep -E '^%s' | tail -n +2 | sort", deviceName, deviceName)
	output, err := sshClient.RunCommand(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition info for device %s: %v", deviceName, err)
	}

	// Parse the output to get partition names and sizes
	lines := strings.Split(strings.TrimSpace(output), "\n")
	var partitionInfos []DevicePartitionInfo

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			// Split by whitespace to get name and size
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				partitionName := parts[0]
				if partitionName != deviceName {
					sizeStr := parts[1]
					size, err := strconv.ParseInt(sizeStr, 10, 64)
					if err != nil {
						log.Warn("Failed to parse size for partition %s: %v", partitionName, err)
						size = 0
					}

					// Find the corresponding by-id name for this partition
					partitionByIdCmd := fmt.Sprintf("ls -la /dev/disk/by-id/ | grep '%s$' | head -1 | awk '{print $9}'", partitionName)
					partitionByIdOutput, err := sshClient.RunCommand(partitionByIdCmd)
					if err != nil {
						log.Warn("Failed to get by-id name for partition %s: %v", partitionName, err)
						// Fall back to regular partition name if by-id lookup fails
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
							// Fall back to regular partition name if by-id name is empty
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

	return partitionInfos, nil
}

// getDeviceByIdName gets the /dev/disk/by-id/ name for a device
func (c *Config) getDeviceByIdName(hvUUID, deviceName string) (string, error) {
	hv, found := c.GetHypervisor(hvUUID)
	if !found {
		return "", fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Create SSH connection to hypervisor
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return "", fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	// Find the by-id name for the device
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

// CreatePhysicalPartition creates an actual partition on the physical device
func (c *Config) CreatePhysicalPartition(hvUUID, deviceName string, size int64) error {
	hv, found := c.GetHypervisor(hvUUID)
	if !found {
		return fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Create SSH connection to hypervisor
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	// Get the full device path
	devicePath := fmt.Sprintf("/dev/%s", deviceName)

	// Check if device exists and get current partition info
	checkCmd := fmt.Sprintf("test -b %s && echo 'exists' || echo 'not found'", devicePath)
	result, err := sshClient.RunCommand(checkCmd)
	if err != nil {
		return fmt.Errorf("failed to check device %s: %v", devicePath, err)
	}
	if strings.TrimSpace(result) != "exists" {
		return fmt.Errorf("device %s not found on hypervisor %s", devicePath, hv.IPAddrs)
	}

	// Get current partition table info
	partInfoCmd := fmt.Sprintf("parted -s %s print 2>/dev/null || echo 'no partitions'", devicePath)
	partInfo, err := sshClient.RunCommand(partInfoCmd)
	if err != nil {
		return fmt.Errorf("failed to get partition info for %s: %v", devicePath, err)
	}

	// Calculate partition number and start position
	partitionNumber := 1
	startOffsetMB := int64(1) // Start at 1MB to avoid conflicts with partition table

	if !strings.Contains(partInfo, "no partitions") {
		// Parse existing partitions to find next available number and position
		lines := strings.Split(partInfo, "\n")
		maxEndMB := int64(1)

		for _, line := range lines {
			// Look for partition entries (format: " 1    1049kB  1075MB  1074MB  ext4")
			if strings.Contains(line, "MB") && (strings.Contains(line, "primary") || len(strings.Fields(line)) >= 4) {
				fields := strings.Fields(line)
				if len(fields) >= 4 {
					partitionNumber++
					// Parse end position to calculate next start
					endStr := fields[3]
					if strings.Contains(endStr, "MB") {
						endStr = strings.TrimSuffix(endStr, "MB")
						if endMB, err := strconv.ParseFloat(endStr, 64); err == nil {
							if int64(endMB) > maxEndMB {
								maxEndMB = int64(endMB)
							}
						}
					}
				}
			}
		}
		startOffsetMB = maxEndMB + 1 // Start after the last partition
	}

	// Convert size from bytes to MB for parted
	sizeMB := size / (1024 * 1024)
	if sizeMB == 0 {
		sizeMB = 1 // Minimum 1MB
	}

	endOffsetMB := startOffsetMB + sizeMB

	// Initialize partition table if none exists
	if strings.Contains(partInfo, "no partitions") {
		initCmd := fmt.Sprintf("parted -s %s mklabel gpt", devicePath)
		_, err = sshClient.RunCommand(initCmd)
		if err != nil {
			return fmt.Errorf("failed to initialize partition table on %s: %v", devicePath, err)
		}
	}

	// Create the partition with proper start/end positions
	partCmd := fmt.Sprintf("parted -s %s mkpart primary %dMB %dMB",
		devicePath,
		startOffsetMB,
		endOffsetMB)

	_, err = sshClient.RunCommand(partCmd)
	if err != nil {
		return fmt.Errorf("failed to create partition on %s: %v", devicePath, err)
	}

	// Wait for partition to be recognized by kernel
	time.Sleep(2 * time.Second)

	// Verify partition was created
	verifyCmd := fmt.Sprintf("parted -s %s print | grep '%d'", devicePath, partitionNumber)
	verifyResult, err := sshClient.RunCommand(verifyCmd)
	if err != nil || strings.TrimSpace(verifyResult) == "" {
		return fmt.Errorf("partition creation verification failed for %s", devicePath)
	}

	return nil
}

// ListPhysicalPartitions lists all partitions on a physical device
func (c *Config) ListPhysicalPartitions(hvUUID, deviceName string) ([]string, error) {
	hv, found := c.GetHypervisor(hvUUID)
	if !found {
		return nil, fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Create SSH connection to hypervisor
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	// Get the full device path
	devicePath := fmt.Sprintf("/dev/%s", deviceName)

	// List partitions using parted
	listCmd := fmt.Sprintf("parted -s %s print 2>/dev/null | grep -E '^ *[0-9]+' | awk '{print $1}' || echo 'no partitions'", devicePath)
	result, err := sshClient.RunCommand(listCmd)
	if err != nil {
		return nil, fmt.Errorf("failed to list partitions for %s: %v", devicePath, err)
	}

	var partitions []string
	if strings.TrimSpace(result) != "no partitions" && strings.TrimSpace(result) != "" {
		lines := strings.Split(strings.TrimSpace(result), "\n")
		for _, line := range lines {
			if line = strings.TrimSpace(line); line != "" {
				partitions = append(partitions, line)
			}
		}
	}

	return partitions, nil
}

/*
// AddDevicePartition adds a NISD partition to a device
func (c *Config) AddDevicePartition(hvUUID, deviceName, nisdInstance string, startOffset, size int64) error {
	_, found := c.GetHypervisor(hvUUID)
	if !found {
		return fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Create physical partition on the device first
	err := c.CreatePhysicalPartition(hvUUID, deviceName, size)
	if err != nil {
		return fmt.Errorf("failed to create physical partition: %v", err)
	}

	partition := DevicePartition{
		PartitionUUID: uuid.New().String(),
		NISDUUID:      nisdInstance,
		Size:          size,
	}

	// Find device and add partition
	for i, pdu := range c.PDUs {
		for j, rack := range pdu.Racks {
			for k, hypervisor := range rack.Hypervisors {
				if hypervisor.ID == hvUUID {
					for devIndex, dev := range hypervisor.Dev {
						if dev.Name == deviceName {
							c.PDUs[i].Racks[j].Hypervisors[k].Dev[devIndex].Partitions = append(
								c.PDUs[i].Racks[j].Hypervisors[k].Dev[devIndex].Partitions, partition)
							return nil
						}
					}
				}
			}
		}
	}

	// Check legacy hypervisors
	for hvIndex, hypervisor := range c.Hypervisors {
		if hypervisor.ID == hvUUID {
			for devIndex, dev := range hypervisor.Dev {
				if dev.Name == deviceName {
					c.Hypervisors[hvIndex].Dev[devIndex].Partitions = append(
						c.Hypervisors[hvIndex].Dev[devIndex].Partitions, partition)
					return nil
				}
			}
		}
	}

	return fmt.Errorf("device %s not found on hypervisor %s", deviceName, hvUUID)
}

*/

// AddMultipleDevicePartitions creates multiple equal-sized NISD partitions on a device
func (c *Config) AddMultipleDevicePartitions(hvUUID, deviceName string, numPartitions int) ([]DevicePartition, error) {
	_, found := c.GetHypervisor(hvUUID)
	if !found {
		return nil, fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Get device size to calculate partition sizes
	deviceSize, err := c.GetDeviceSize(hvUUID, deviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get device size: %v", err)
	}

	// Create multiple physical partitions
	err = c.CreateMultipleEqualPartitions(hvUUID, deviceName, numPartitions)
	if err != nil {
		return nil, fmt.Errorf("failed to create physical partitions: %v", err)
	}

	// Get actual partition names after creation
	actualPartitionNames, err := c.GetDevicePartitionNames(hvUUID, deviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition names: %v", err)
	}

	if len(actualPartitionNames) != numPartitions {
		return nil, fmt.Errorf("expected %d partitions but found %d", numPartitions, len(actualPartitionNames))
	}

	// Calculate partition size (excluding 1MB for partition table)
	usableSize := deviceSize - (1024 * 1024)
	partitionSize := usableSize / int64(numPartitions)

	var createdPartitions []DevicePartition

	// Create NISD partitions for each physical partition
	for i := 0; i < numPartitions; i++ {

		// Generate NISD instance and UUID
		nisdInstance := uuid.New().String()

		partition := DevicePartition{
			PartitionID:   actualPartitionNames[i],
			PartitionPath: actualPartitionNames[i],
			NISDUUID:      nisdInstance,
			Size:          partitionSize,
		}

		createdPartitions = append(createdPartitions, partition)
	}

	// Add all partitions to the device
	for i, pdu := range c.PDUs {
		for j, rack := range pdu.Racks {
			for k, hypervisor := range rack.Hypervisors {
				if hypervisor.ID == hvUUID {
					for devIndex, dev := range hypervisor.Dev {
						if dev.Name == deviceName {
							// Clear existing partitions and add new ones
							c.PDUs[i].Racks[j].Hypervisors[k].Dev[devIndex].Partitions = createdPartitions
							return createdPartitions, nil
						}
					}
				}
			}
		}
	}

	// Check legacy hypervisors
	for hvIndex, hypervisor := range c.Hypervisors {
		if hypervisor.ID == hvUUID {
			for devIndex, dev := range hypervisor.Dev {
				if dev.Name == deviceName {
					// Clear existing partitions and add new ones
					c.Hypervisors[hvIndex].Dev[devIndex].Partitions = createdPartitions
					return createdPartitions, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("device %s not found on hypervisor %s", deviceName, hvUUID)
}

// DeletePhysicalPartition removes an actual partition from the physical device
func (c *Config) DeletePhysicalPartition(hvUUID, deviceName string, partitionNumber int) error {
	hv, found := c.GetHypervisor(hvUUID)
	if !found {
		return fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Create SSH connection to hypervisor
	sshClient, err := NewSSHClient(hv.IPAddrs)
	if err != nil {
		return fmt.Errorf("failed to connect to hypervisor %s: %v", hv.IPAddrs, err)
	}
	defer sshClient.Close()

	// Get the full device path
	devicePath := fmt.Sprintf("/dev/%s", deviceName)

	// Check if device exists
	checkCmd := fmt.Sprintf("test -b %s && echo 'exists' || echo 'not found'", devicePath)
	result, err := sshClient.RunCommand(checkCmd)
	if err != nil {
		return fmt.Errorf("failed to check device %s: %v", devicePath, err)
	}
	if strings.TrimSpace(result) != "exists" {
		return fmt.Errorf("device %s not found on hypervisor %s", devicePath, hv.IPAddrs)
	}

	// Check if partition exists
	checkPartCmd := fmt.Sprintf("parted -s %s print | grep '^ %d '", devicePath, partitionNumber)
	partResult, err := sshClient.RunCommand(checkPartCmd)
	if err != nil || strings.TrimSpace(partResult) == "" {
		return fmt.Errorf("partition %d not found on device %s", partitionNumber, devicePath)
	}

	// Delete the partition using parted
	deleteCmd := fmt.Sprintf("parted -s %s rm %d", devicePath, partitionNumber)
	_, err = sshClient.RunCommand(deleteCmd)
	if err != nil {
		return fmt.Errorf("failed to delete partition %d from %s: %v", partitionNumber, devicePath, err)
	}

	// Wait for kernel to update
	time.Sleep(2 * time.Second)

	// Verify partition was deleted
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

// RemoveDevicePartition removes a NISD partition from a device
func (c *Config) RemoveDevicePartition(hvUUID, deviceName, partitionID string) error {
	// Check hierarchical structure
	for pduIndex, pdu := range c.PDUs {
		for rackIndex, rack := range pdu.Racks {
			for hvIndex, hv := range rack.Hypervisors {
				if hv.ID == hvUUID {
					for devIndex, dev := range hv.Dev {
						if dev.Name == deviceName {
							for partIndex, partition := range dev.Partitions {
								if partition.PartitionID == partitionID {
									// Delete physical partition first (partition numbers are 1-based)
									physicalPartNum := partIndex + 1
									err := c.DeletePhysicalPartition(hvUUID, deviceName, physicalPartNum)
									if err != nil {
										// Log warning but continue with logical removal
										fmt.Printf("Warning: failed to delete physical partition %d: %v\n", physicalPartNum, err)
									}

									// Remove partition from configuration
									c.PDUs[pduIndex].Racks[rackIndex].Hypervisors[hvIndex].Dev[devIndex].Partitions =
										append(dev.Partitions[:partIndex], dev.Partitions[partIndex+1:]...)
									return nil
								}
							}
						}
					}
				}
			}
		}
	}

	// Check legacy hypervisors
	for hvIndex, hypervisor := range c.Hypervisors {
		if hypervisor.ID == hvUUID {
			for devIndex, dev := range hypervisor.Dev {
				if dev.Name == deviceName {
					for partIndex, partition := range dev.Partitions {
						if partition.PartitionID == partitionID {
							// Delete physical partition first (partition numbers are 1-based)
							physicalPartNum := partIndex + 1
							err := c.DeletePhysicalPartition(hvUUID, deviceName, physicalPartNum)
							if err != nil {
								// Log warning but continue with logical removal
								fmt.Printf("Warning: failed to delete physical partition %d: %v\n", physicalPartNum, err)
							}

							// Remove partition from configuration
							c.Hypervisors[hvIndex].Dev[devIndex].Partitions =
								append(dev.Partitions[:partIndex], dev.Partitions[partIndex+1:]...)
							return nil
						}
					}
				}
			}
		}
	}

	return fmt.Errorf("partition %s not found on device %s on hypervisor %s", partitionID, deviceName, hvUUID)
}

// GetAllPartitionsForNISD returns all partitions available for NISD initialization
func (c *Config) GetAllPartitionsForNISD() []struct {
	HvUUID    string
	HvName    string
	Device    Device
	Partition DevicePartition
} {
	var result []struct {
		HvUUID    string
		HvName    string
		Device    Device
		Partition DevicePartition
	}

	// Check hierarchical structure
	for _, pdu := range c.PDUs {
		for _, rack := range pdu.Racks {
			for _, hv := range rack.Hypervisors {
				for _, device := range hv.Dev {
					for _, partition := range device.Partitions {
						result = append(result, struct {
							HvUUID    string
							HvName    string
							Device    Device
							Partition DevicePartition
						}{
							HvUUID:    hv.ID,
							HvName:    hv.Name,
							Device:    device,
							Partition: partition,
						})
					}
				}
			}
		}
	}

	// Check legacy hypervisors
	for _, hv := range c.Hypervisors {
		for _, device := range hv.Dev {
			for _, partition := range device.Partitions {
				result = append(result, struct {
					HvUUID    string
					HvName    string
					Device    Device
					Partition DevicePartition
				}{
					HvUUID:    hv.ID,
					HvName:    hv.Name,
					Device:    device,
					Partition: partition,
				})
			}
		}
	}

	return result
}

// GetAllInitializedNISDs returns all initialized NISD instances
// In a real implementation, this would query the control plane for registered NISDs
func (c *Config) GetAllInitializedNISDs() []ctlplfl.Nisd {
	var result []ctlplfl.Nisd

	// TODO: In a real implementation, this would query the control plane
	// For now, we'll return an empty list as a placeholder
	// This could be implemented by calling a control plane API to get all registered NISDs

	return result
}

// GetAllDevicesWithPartitions returns all devices that can have partitions (initialized devices)
func (c *Config) GetAllDevicesWithPartitions() []struct {
	HvUUID string
	HvName string
	Device Device
} {
	var devices []struct {
		HvUUID string
		HvName string
		Device Device
	}

	// Check hierarchical structure
	for _, pdu := range c.PDUs {
		for _, rack := range pdu.Racks {
			for _, hv := range rack.Hypervisors {
				for _, dev := range hv.Dev {
					if dev.State == ctlplfl.INITIALIZED {
						devices = append(devices, struct {
							HvUUID string
							HvName string
							Device Device
						}{
							HvUUID: hv.ID,
							HvName: hv.Name,
							Device: dev,
						})
					}
				}
			}
		}
	}

	// Check legacy hypervisors
	for _, hv := range c.Hypervisors {
		for _, dev := range hv.Dev {
			if dev.State == ctlplfl.INITIALIZED {
				devices = append(devices, struct {
					HvUUID string
					HvName string
					Device Device
				}{
					HvUUID: hv.ID,
					HvName: hv.Name,
					Device: dev,
				})
			}
		}
	}

	return devices
}

// GetAllDevicesForPartitioning returns all devices (initialized or not) that can be partitioned
func (c *Config) GetAllDevicesForPartitioning() []struct {
	HvUUID string
	HvName string
	Device Device
} {
	var devices []struct {
		HvUUID string
		HvName string
		Device Device
	}

	// Check hierarchical structure
	for _, pdu := range c.PDUs {
		for _, rack := range pdu.Racks {
			for _, hv := range rack.Hypervisors {
				for _, dev := range hv.Dev {
					// Include all devices, not just initialized ones
					devices = append(devices, struct {
						HvUUID string
						HvName string
						Device Device
					}{
						HvUUID: hv.ID,
						HvName: hv.Name,
						Device: dev,
					})
				}
			}
		}
	}

	// Check legacy hypervisors
	for _, hv := range c.Hypervisors {
		for _, dev := range hv.Dev {
			// Include all devices, not just initialized ones
			devices = append(devices, struct {
				HvUUID string
				HvName string
				Device Device
			}{
				HvUUID: hv.ID,
				HvName: hv.Name,
				Device: dev,
			})
		}
	}

	return devices
}

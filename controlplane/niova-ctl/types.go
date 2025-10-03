package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
)

// Type aliases to use libctlplanefuncs types
type Device = ctlplfl.Device
type DevicePartition = ctlplfl.DevicePartition
type PDU = ctlplfl.PDU
type Rack = ctlplfl.Rack
type Hypervisor = ctlplfl.Hypervisor

type Config struct {
	PDUs []ctlplfl.PDU `json:"pdus"`
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

	//TODO query control plane to get existing PDUs
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
					if existing.IPAddress == hv.IPAddress {
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
		if existing.IPAddress == hv.IPAddress {
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
// avoiding already allocated ports
func (c *Config) AllocatePortPair(hypervisorUUID string, portRange string) (int, int, error) {
	startPort, endPort, err := ParsePortRange(portRange)
	if err != nil {
		return 0, 0, err
	}

	// Collect all allocated ports for this hypervisor
	allocatedPorts := make(map[int]bool)

	// Check hierarchical structure
	for _, pdu := range c.PDUs {
		for _, rack := range pdu.Racks {
			for _, hv := range rack.Hypervisors {
				if hv.ID == hypervisorUUID {
					for _, dev := range hv.Dev {
						// Include partition ports
						for _, partition := range dev.Partitions {
							if partition.ClientPort != 0 {
								allocatedPorts[partition.ClientPort] = true
							}
							if partition.ServerPort != 0 {
								allocatedPorts[partition.ServerPort] = true
							}
						}
					}
				}
			}
		}
	}

	// Check legacy hypervisors for backward compatibility
	for _, hv := range c.Hypervisors {
		if hv.ID == hypervisorUUID {
			for _, dev := range hv.Dev {
				for _, partition := range dev.Partitions {
					if partition.ClientPort != 0 {
						allocatedPorts[partition.ClientPort] = true
					}
					if partition.ServerPort != 0 {
						allocatedPorts[partition.ServerPort] = true
					}
				}
			}
		}
	}

	// Find two consecutive available ports
	for port := startPort; port < endPort-1; port += 2 {
		clientPort := port
		serverPort := port + 1

		if !allocatedPorts[clientPort] && !allocatedPorts[serverPort] {
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
						if dev.Name == deviceName && !dev.Initialized {

							// Allocate ports
							_, _, err := c.AllocatePortPair(hypervisorUUID, hv.PortRange)
							if err != nil {
								return fmt.Errorf("failed to allocate ports: %v", err)
							}

							// Build hierarchical failure domain
							hierarchicalFailureDomain := fmt.Sprintf("%s/%s/%s/%s", pdu.Name, rack.Name, hv.Name, failureDomain)

							// Update device
							c.PDUs[i].Racks[j].Hypervisors[k].Dev[devIndex] = Device{
								ID:             dev.ID,
								Name:           dev.Name,
								Size:           dev.Size,
								HypervisorID: hypervisorUUID,
								DevicePath:     fmt.Sprintf("/dev/%s", dev.Name),
								FailureDomain:  hierarchicalFailureDomain,
								Initialized:    true,
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
				if dev.Name == deviceName && !dev.Initialized {

					// Allocate ports
					_, _, err := c.AllocatePortPair(hypervisorUUID, hv.PortRange)
					if err != nil {
						return fmt.Errorf("failed to allocate ports: %v", err)
					}

					// Update device
					c.Hypervisors[hvIndex].Dev[devIndex] = Device{
						ID:             dev.ID,
						Name:           dev.Name,
						Size:           dev.Size,
						HypervisorID: hypervisorUUID,
						DevicePath:     fmt.Sprintf("/dev/%s", dev.Name),
						FailureDomain:  failureDomain,
						Initialized:    true,
					}

					return nil
				}
			}
			return fmt.Errorf("device %s not found or already initialized", deviceName)
		}
	}
	return fmt.Errorf("hypervisor with UUID %s not found", hypervisorUUID)
}

// AddDevicePartition adds a NISD partition to a device
func (c *Config) AddDevicePartition(hvUUID, deviceName, nisdInstance string, startOffset, size int64) error {
	hv, found := c.GetHypervisor(hvUUID)
	if !found {
		return fmt.Errorf("hypervisor with UUID %s not found", hvUUID)
	}

	// Allocate ports for partition
	clientPort, serverPort, err := c.AllocatePortPair(hvUUID, hv.PortRange)
	if err != nil {
		return fmt.Errorf("failed to allocate ports for partition: %v", err)
	}

	partition := DevicePartition{
		PartitionUUID: uuid.New().String(),
		NISDInstance:  nisdInstance,
		StartOffset:   startOffset,
		Size:          size,
		ClientPort:    clientPort,
		ServerPort:    serverPort,
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

// RemoveDevicePartition removes a NISD partition from a device
func (c *Config) RemoveDevicePartition(hvUUID, deviceName, partitionUUID string) error {
	// Check hierarchical structure
	for pduIndex, pdu := range c.PDUs {
		for rackIndex, rack := range pdu.Racks {
			for hvIndex, hv := range rack.Hypervisors {
				if hv.ID == hvUUID {
					for devIndex, dev := range hv.Dev {
						if dev.Name == deviceName {
							for partIndex, partition := range dev.Partitions {
								if partition.PartitionUUID == partitionUUID {
									// Remove partition
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
						if partition.PartitionUUID == partitionUUID {
							// Remove partition
							c.Hypervisors[hvIndex].Dev[devIndex].Partitions =
								append(dev.Partitions[:partIndex], dev.Partitions[partIndex+1:]...)
							return nil
						}
					}
				}
			}
		}
	}

	return fmt.Errorf("partition %s not found on device %s on hypervisor %s", partitionUUID, deviceName, hvUUID)
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
					if dev.Initialized {
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
			if dev.Initialized {
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

// GenerateNISDInstance generates a unique NISD instance name
func (c *Config) GenerateNISDInstance() string {
	existingInstances := make(map[string]bool)

	// Collect all existing NISD instances
	for _, pdu := range c.PDUs {
		for _, rack := range pdu.Racks {
			for _, hv := range rack.Hypervisors {
				for _, dev := range hv.Dev {
					for _, partition := range dev.Partitions {
						existingInstances[partition.NISDInstance] = true
					}
				}
			}
		}
	}

	// Check legacy hypervisors
	for _, hv := range c.Hypervisors {
		for _, dev := range hv.Dev {
			for _, partition := range dev.Partitions {
				existingInstances[partition.NISDInstance] = true
			}
		}
	}

	// Generate a unique instance name
	for i := 1; ; i++ {
		instance := fmt.Sprintf("nisd-%03d", i)
		if !existingInstances[instance] {
			return instance
		}
	}
}

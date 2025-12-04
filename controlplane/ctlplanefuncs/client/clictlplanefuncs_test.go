package clictlplanefuncs

import (
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"context"
    "time"
    "golang.org/x/sync/errgroup"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Global maps to store test results for reuse between tests
var (
	PDUs  = make(map[string]cpLib.PDU)
	Racks = make(map[string]cpLib.Rack)
	Hypervisors = make(map[string]cpLib.Hypervisor)
	Devices = make(map[string]cpLib.Device)
	Nisds = make(map[string]cpLib.Nisd)
)

var VDEV_ID string

func newClient(t *testing.T) *CliCFuncs {

	clusterID := os.Getenv("RAFT_ID")
	if clusterID == "" {
		log.Fatal("RAFT_ID env variable not set")
	}

	configPath := os.Getenv("GOSSIP_NODES_PATH")
	if configPath == "" {
		log.Fatal("GOSSIP_NODES_PATH env variable not set")
	}
	c := InitCliCFuncs(
		uuid.New().String(),
		clusterID,
		configPath,
	)
	if c == nil {
		t.Fatal("failed to init client funcs")
	}
	return c
}

func TestPutAndGetSinglePDU(t *testing.T) {
	c := newClient(t) 

	mockNisd := []cpLib.Nisd{
		{
			ClientPort: 7001,
			PeerPort:   8001,
			ID:         "nisd-001",
			FailureDomain: []string{
				"pdu-01",
				"rack-01",
				"hv-01",
				"dev-001",
			},
	nisd := cpLib.Nisd{
			ClientPort:    7001,
			PeerPort:      8001,
			ID:            "nisd-001",
			DevID:         "dev-001",
			HyperVisorID:  "hv-01",
			FailureDomain: "fd-01",
			IPAddr:        "192.168.1.10",
			TotalSize:     1_000_000_000_000, // 1 TB
			AvailableSize: 750_000_000_000,   // 750 GB
		},
		{
			ClientPort: 7002,
			PeerPort:   8002,
			ID:         "nisd-002",
			FailureDomain: []string{
				"pdu-02",
				"rack-02",
				"hv-02",
				"dev-002",
			},
			IPAddr:        "192.168.1.11",
			TotalSize:     500_000_000_000, // 500 GB
			AvailableSize: 200_000_000_000, // 200 GB
		},
		{
			ClientPort: 7003,
			PeerPort:   8003,
			ID:         "nisd-003",
			FailureDomain: []string{
				"pdu-03",
				"rack-03",
				"hv-03",
				"dev-003",
			},
			IPAddr:        "192.168.1.12",
			TotalSize:     2_000_000_000_000, // 2 TB
			AvailableSize: 1_500_000_000_000, // 1.5 TB
		},
	}

	for _, n := range mockNisd {
		resp, err := c.PutNisd(&n)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	res, err := c.GetNisd(cpLib.GetReq{ID: "nisd-002"})
	log.Info("GetNisds: ", res)
	assert.NoError(t, err)

}

func TestPutAndGetDevice(t *testing.T) {
	c := newClient(t)

	mockDevices := []cpLib.Device{
		{
			ID:            "6qp847cd0-ab3e-11f0-aa15-1f40dd976538",
			SerialNumber:  "SN123456789",
			State:         1,
			HypervisorID:  "hv-01",
			FailureDomain: "fd-01",
			DevicePath:    "/temp/path1",
			Name:          "dev-1",
		},
		{
			ID:            "6bd604a6-ab3e-11f0-805a-3f086c1f2d21",
			SerialNumber:  "SN987654321",
			State:         0,
			HypervisorID:  "hv-02",
			FailureDomain: "fd-01",
			DevicePath:    "/temp/path2",
			Name:          "dev-2",
			Size:          12345689,
			Partitions: []cpLib.DevicePartition{cpLib.DevicePartition{
				PartitionID:   "b97c34qwe-9775558a141a",
				PartitionPath: "/part/path1",
				NISDUUID:      "1",
				DevID:         "60447cdsad0-ab3e-1342340dd976538",
				Size:          123467,
			}, cpLib.DevicePartition{
				PartitionID:   "b97c3464-ab3e-11f0-b32d-977555asdsa",
				PartitionPath: "/part/path2",
				NISDUUID:      "1",
				DevID:         "60447csdd0-ab3e-11f0-aa15-1f402342538",
				Size:          123467,
			},
			},
		},
		{
			ID:            "60447cd0-ab3e-11f0-aa15-1f40dd976538",
			SerialNumber:  "SN112233445",
			State:         2,
			HypervisorID:  "hv-01",
			FailureDomain: "fd-02",
			DevicePath:    "/temp/path3",
			Name:          "dev-3",
			Size:          9999999,
			Partitions: []cpLib.DevicePartition{cpLib.DevicePartition{
				PartitionID:   "b97c3464-ab3e-11f0-b32d-9775558a141a",
				PartitionPath: "/part/path3",
				NISDUUID:      "1",
				DevID:         "60447cd0-ab3e-11f0-aa15-1f40dd976538",
				Size:          123467,
			},
			},
		},
	}

	for _, p := range mockDevices {
		resp, err := c.PutDevice(&p)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}
	}

	// PUT single PDU
	resp, err := c.PutPDU(&pdu)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// GET operation 
	res, err := c.GetPDUs(&cpLib.GetReq{GetAll: true})
	log.Info("Single PDU: ", res)
	assert.NoError(t, err)
	assert.NotEmpty(t, res)
	
	returned := res[0]

	assert.Equal(t, pdu.Name, returned.Name, "Mismatch in Name for PDU %s", pdu.ID)
	assert.Equal(t, pdu.Location, returned.Location, "Mismatch in Location for PDU %s", pdu.ID)
	assert.Equal(t, pdu.PowerCapacity, returned.PowerCapacity, "Mismatch in Power Capacity for PDU %s", pdu.ID)
	assert.Equal(t, pdu.Specification, returned.Specification, "Mismatch in Specification for PDU %s", pdu.ID)

	log.Infof("Single PDU PUT/GET validation successful for %s", pdu.ID)
}

func TestPutAndGetMultiplePDUs(t *testing.T) {
	c := newClient(t)

	pdus := []cpLib.PDU{
		{ID: "95f62aee-997e-11f0-9f1b-a70cff4b660b",
			Name:          "pdu-1",
			Location:      "us-east",
			PowerCapacity: "15Kw",
			Specification: "specification1",
		},
		{ID: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea",
			Name:          "pdu-2",
			Location:      "us-west",
			PowerCapacity: "15Kw",
			Specification: "specification2",
		},
	}

	// PUT multiple PDUs
	for _, p := range pdus {
		resp, err := c.PutPDU(&p)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	// GET all PDUs
	res, err := c.GetPDUs(&cpLib.GetReq{GetAll: true})
	assert.NoError(t, err)
	assert.Equal(t, len(pdus), len(res), "Expected %d PDUs but got %d", len(pdus), len(res))

	// Store results in the map
	for _, p := range res {
		PDUs[p.ID] = p
	}

	log.Infof("Validated all %d PDUs successfully", len(pdus))
}

func TestPutAndGetSingleRack(t *testing.T) {
	c := newClient(t)

	rack := cpLib.Rack{
		ID:            "8a5303ae-ab23-11f0-bb87-632ad3e09c04",
		PDUID:         "95f62aee-997e-11f0-9f1b-a70cff4b660b",
		Name:          "rack-1",
		Location:      "us-east",
		Specification: "rack1-spec",
	}

	// PUT single rack
	resp, err := c.PutRack(&rack)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// GET the same rack by ID
	res, err := c.GetRacks(&cpLib.GetReq{ID: rack.ID})
	assert.NoError(t, err)
	assert.NotEmpty(t, res)

	returned := res[0]

	// Validate all fields
	assert.Equal(t, rack.ID, returned.ID)
	assert.Equal(t, rack.PDUID, returned.PDUID)
	assert.Equal(t, rack.Name, returned.Name)
	assert.Equal(t, rack.Location, returned.Location)
	assert.Equal(t, rack.Specification, returned.Specification)

	log.Infof("Single Rack PUT/GET validation successful for Rack ID: %s", rack.ID)
}

func TestPutAndGetMultipleRacks(t *testing.T) {
	c := newClient(t)

	racks := []cpLib.Rack{
		{ID: "8a5303ae-ab23-11f0-bb87-632ad3e09c04", PDUID: "95f62aee-997e-11f0-9f1b-a70cff4b660b", Name: "rack-1", Location: "us-east", Specification: "rack1-spec"},
		{ID: "93e2925e-ab23-11f0-958d-87f55a6a9981", PDUID: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea", Name: "rack-2", Location: "us-west", Specification: "rack2-spec"},
	}

	for _, r := range racks {
		resp, err := c.PutRack(&r)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	resp, err := c.GetRacks(&cpLib.GetReq{})
	assert.NoError(t, err)
	assert.Equal(t, len(racks), len(resp), "Expected %d racks but got %d", len(racks), len(resp))

	// Store results in the shared map
	for _, r := range resp {
		Racks[r.ID] = r
	}

	for _, rack := range Racks {
		pdu, exists := PDUs[rack.PDUID]
		assert.True(t, exists, "Rack %s references PDUID %s which does not exist in PDUs", rack.Name, rack.PDUID)

		if exists {
			assert.Equal(t, pdu.Location, rack.Location,
				"Location mismatch between Rack %s (Location: %s) and PDU %s (Location: %s)",
				rack.Name, rack.Location, pdu.Name, pdu.Location)
		}
	}

	log.Infof("Validated %d Rack↔PDU associations successfully", len(Racks))

	log.Infof("All %d racks validated successfully", len(racks))
}

func TestPutAndGetSingleHypervisor(t *testing.T) {
	c := newClient(t)

	hv := cpLib.Hypervisor{
		RackID:     "rack-1",
		ID:         "89944570-ab2a-11f0-b55d-8fc2c05d35f4",
		IPAddress:  "127.0.0.1",
		PortRange:  "8000-9000",
		SSHPort:    "6999",
		Name:       "hv-1",
	}

	// Put one hypervisor
	putResp, err := c.PutHypervisor(&hv)
	assert.NoError(t, err, "Error while putting hypervisor")
	assert.True(t, putResp.Success, "PutHypervisor response not successful")

	// Get the same hypervisor by ID
	getResp, err := c.GetHypervisor(&cpLib.GetReq{ID: hv.ID})
	assert.NoError(t, err, "Error while getting hypervisor by ID")
	assert.NotNil(t, getResp, "Expected non-nil response for GetHypervisor")

	// Validate returned fields
	assert.Equal(t, hv.ID, getResp[0].ID)
	assert.Equal(t, hv.Name, getResp[0].Name)
	assert.Equal(t, hv.RackID, getResp[0].RackID)
	assert.Equal(t, hv.IPAddress, getResp[0].IPAddress)
	assert.Equal(t, hv.PortRange, getResp[0].PortRange)
	assert.Equal(t, hv.SSHPort, getResp[0].SSHPort)

	log.Infof("Single Hypervisor PUT/GET validation successful for Hypervisor ID: %s", hv.ID)	
}

func TestPutAndGetMultipleHypervisors(t *testing.T) {
	c := newClient(t)

	hypervisors := []cpLib.Hypervisor{
		{RackID: "rack-1", ID: "89944570-ab2a-11f0-b55d-8fc2c05d35f4", IPAddress: "127.0.0.1", PortRange: "8000-9000", SSHPort: "6999", Name: "hv-1"},
		{RackID: "rack-2", ID: "8f70f2a4-ab2a-11f0-a1bb-cb25e1fa6a6b", IPAddress: "127.0.0.2", PortRange: "5000-7000", SSHPort: "7999", Name: "hv-2"},
	}

	// PUT multiple hypervisor
	for _, hv := range hypervisors {
		resp, err := c.PutHypervisor(&hv)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	// GET all hypervisors
	resp, err := c.GetHypervisor(&cpLib.GetReq{GetAll: true})
	assert.NoError(t, err)
	assert.Equal(t, len(hypervisors), len(resp), "Expected %d hypervisors but got %d", len(hypervisors), len(resp))

	for _, hv := range resp {
		Hypervisors[hv.ID] = hv
	}

	log.Infof("All %d racks validated successfully", len(hypervisors))

	// Validation: ensure inserted hypervisors are present with correct fields
	for _, expected := range hypervisors {
		found := false
		for _, got := range resp {
			if got.ID == expected.ID {
				assert.Equal(t, expected.Name, got.Name, "Mismatch in Name for ID %s", expected.ID)
				assert.Equal(t, expected.RackID, got.RackID, "Mismatch in RackID for ID %s", expected.ID)
				assert.Equal(t, expected.IPAddress, got.IPAddress, "Mismatch in IPAddress for ID %s", expected.ID)
				assert.Equal(t, expected.PortRange, got.PortRange, "Mismatch in PortRange for ID %s", expected.ID)
				assert.Equal(t, expected.SSHPort, got.SSHPort, "Mismatch in SSHPort for ID %s", expected.ID)
				found = true
				break
			}
		}
		assert.True(t, found, "Expected hypervisor with ID %s not found in response", expected.ID)
	}

	// Validation: Hypervisor ↔ Rack
	for _, hv := range Hypervisors {
		var rackExists bool
		for _, rack := range Racks {
			if rack.Name == hv.RackID {
				rackExists = true
				break
			}
		}

		assert.True(t, rackExists, "Hypervisor %s references RackID %s which does not exist in known Racks", hv.Name, hv.RackID)
	}
}

func TestPutAndGetSingleDevice(t *testing.T) {
	c := newClient(t)

	device := cpLib.Device{
		ID:            "60447cd0-ab3e-11f0-aa15-1f40dd976538",
		SerialNumber:  "SN112233445",
		State:         2,
		HypervisorID:  "hv-1",
		FailureDomain: "fd-02",
		DevicePath:    "/temp/path3",
		Name:          "dev-3",
		Size:          9999999,
		Partitions: []cpLib.DevicePartition{
			{
				PartitionID:   "b97c3464-ab3e-11f0-b32d-9775558a141a",
				PartitionPath: "/part/path3",
				NISDUUID:      "1",
				DevID:         "60447cd0-ab3e-11f0-aa15-1f40dd976538",
				Size:          123467,
			},
		},
	}

	// PUT single device
	resp, err := c.PutDevice(&device)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// GET device
	res, err := c.GetDevices(cpLib.GetReq{ID: device.ID})
	log.Infof("fetch single device info: %s", res)
	assert.NoError(t, err)
	assert.NotEmpty(t, res)

	returned := res[0]

	// Validate all key fields
	assert.Equal(t, device.ID, returned.ID)
	assert.Equal(t, device.SerialNumber, returned.SerialNumber)
	assert.Equal(t, device.HypervisorID, returned.HypervisorID)
	assert.Equal(t, device.FailureDomain, returned.FailureDomain)
	assert.Equal(t, device.DevicePath, returned.DevicePath)
	assert.Equal(t, device.Name, returned.Name)
	assert.Equal(t, device.Size, returned.Size)
	assert.Equal(t, len(device.Partitions), len(returned.Partitions))

	for i := range device.Partitions {
		assert.Equal(t, device.Partitions[i].PartitionID, returned.Partitions[i].PartitionID)
		assert.Equal(t, device.Partitions[i].PartitionPath, returned.Partitions[i].PartitionPath)
		assert.Equal(t, device.Partitions[i].NISDUUID, returned.Partitions[i].NISDUUID)
		assert.Equal(t, device.Partitions[i].DevID, returned.Partitions[i].DevID)
		assert.Equal(t, device.Partitions[i].Size, returned.Partitions[i].Size)
	}

	log.Infof("Single Device PUT/GET validation successful for %s", device.ID)
}

func TestPutAndGetMultipleDevices(t *testing.T) {
	c := newClient(t)

	// Step 0: Create a NISD to allocate space for Vdevs
	n := cpLib.Nisd{
		ClientPort: 7001,
		PeerPort:   8001,
		ID:         "nisd-001",
		FailureDomain: []string{
			"pdu-01",
			"rack-01",
			"hv-01",
			"dev-006",
		},
		IPAddr:        "192.168.1.10",
		InitDev:       true,
		TotalSize:     15_000_000_000_000, 
		AvailableSize: 15_000_000_000_000, 
	}

	// PUT operation 
	resp, err := c.PutNisd(&nisd)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// GET operation 
	res, err := c.GetNisds(cpLib.GetReq{ID: nisd.ID})
	log.Info("GetNisdCfg: ", res)
	assert.NoError(t, err)
	assert.Equal(t, len(pdus), len(res), "Expected %d PDUs but got %d", len(pdus), len(res))

	// Validate each PDU field-by-field
	for _, inserted := range pdus {
		var found *cpLib.PDU
		for _, fetched := range res {
			if fetched.ID == inserted.ID {
				found = &fetched
				break
			}
		}

		assert.NotNil(t, found, "Inserted PDU with ID %s not found in response", inserted.ID)

		// Field-level checks
		assert.Equal(t, inserted.Name, found.Name, "Mismatch in Name for PDU %s", inserted.ID)
		assert.Equal(t, inserted.Location, found.Location, "Mismatch in Location for PDU %s", inserted.ID)
		assert.Equal(t, inserted.PowerCapacity, found.PowerCapacity, "Mismatch in PowerCapacity for PDU %s", inserted.ID)
		assert.Equal(t, inserted.Specification, found.Specification, "Mismatch in Specification for PDU %s", inserted.ID)
	}

	log.Infof("Validated all %d PDUs successfully", len(pdus))
	assert.NotEmpty(t, res)
	
	returned := res[0]

	// Validate all key fields match
	assert.Equal(t, nisd.ID, returned.ID, "ID mismatch")
	assert.Equal(t, nisd.DevID, returned.DevID, "DevID mismatch")
	assert.Equal(t, nisd.HyperVisorID, returned.HyperVisorID, "HyperVisorID mismatch")
	assert.Equal(t, nisd.FailureDomain, returned.FailureDomain, "FailureDomain mismatch")
	assert.Equal(t, nisd.IPAddr, returned.IPAddr, "IPAddr mismatch")
	assert.Equal(t, nisd.ClientPort, returned.ClientPort, "ClientPort mismatch")
	assert.Equal(t, nisd.PeerPort, returned.PeerPort, "PeerPort mismatch")
	assert.Equal(t, nisd.TotalSize, returned.TotalSize, "TotalSize mismatch")
	assert.Equal(t, nisd.AvailableSize, returned.AvailableSize, "AvailableSize mismatch")
	assert.Equal(t, nisd.InitDev, returned.InitDev, "InitDev mismatch")

	log.Info("Single NISD PUT/GET validations successful.")
}

func TestPutAndGetSingleNisd(t *testing.T) {
	c := newClient(t)

	nisd := cpLib.Nisd{
			ClientPort:    7001,
			PeerPort:      8001,
			ID:            "nisd-001",
			DevID:         "dev-001",
			HyperVisorID:  "hv-01",
			FailureDomain: "fd-01",
			IPAddr:        "192.168.1.10",
			InitDev:       true,
			TotalSize:     1_000_000_000_000, // 1 TB
			AvailableSize: 750_000_000_000,   // 750 GB
	}

	// PUT operation 
	resp, err := c.PutNisdCfg(&nisd)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// GET operation 
	res, err := c.GetNisdCfg(cpLib.GetReq{ID: nisd.ID})
	log.Info("GetNisdCfg: ", res)
	assert.NoError(t, err)
	assert.NotEmpty(t, res)
	
	returned := res[0]

	// Validate all key fields match
	assert.Equal(t, nisd.ID, returned.ID, "ID mismatch")
	assert.Equal(t, nisd.DevID, returned.DevID, "DevID mismatch")
	assert.Equal(t, nisd.HyperVisorID, returned.HyperVisorID, "HyperVisorID mismatch")
	assert.Equal(t, nisd.FailureDomain, returned.FailureDomain, "FailureDomain mismatch")
	assert.Equal(t, nisd.IPAddr, returned.IPAddr, "IPAddr mismatch")
	assert.Equal(t, nisd.ClientPort, returned.ClientPort, "ClientPort mismatch")
	assert.Equal(t, nisd.PeerPort, returned.PeerPort, "PeerPort mismatch")
	assert.Equal(t, nisd.TotalSize, returned.TotalSize, "TotalSize mismatch")
	assert.Equal(t, nisd.AvailableSize, returned.AvailableSize, "AvailableSize mismatch")
	assert.Equal(t, nisd.InitDev, returned.InitDev, "InitDev mismatch")

	log.Info("Single NISD PUT/GET validations successful.")
}

func TestPutAndGetMultipleNisds(t *testing.T) {
	c := newClient(t)

	mockNisd := []cpLib.Nisd{
		{
			ClientPort:    7001,
			PeerPort:      8001,
			ID:            "nisd-001",
			DevID:         "dev-1",
			HyperVisorID:  "hv-1",
			FailureDomain: "fd-01",
			IPAddr:        "192.168.1.10",
			InitDev:       true,
			TotalSize:     1_000_000_000_000,
			AvailableSize: 750_000_000_000,
		},
		{
			ClientPort:    7002,
			PeerPort:      8002,
			ID:            "nisd-002",
			DevID:         "dev-2",
			HyperVisorID:  "hv-1",
			FailureDomain: "fd-02",
			IPAddr:        "192.168.1.11",
			InitDev:       false,
			TotalSize:     500_000_000_000,
			AvailableSize: 200_000_000_000,
		},
		{
			ClientPort:    7003,
			PeerPort:      8003,
			ID:            "nisd-003",
			DevID:         "dev-3",
			HyperVisorID:  "hv-2",
			FailureDomain: "fd-01",
			IPAddr:        "192.168.1.12",
			InitDev:       true,
			TotalSize:     2_000_000_000_000,
			AvailableSize: 1_500_000_000_000,
		},
	}

	// PUT multiple NISDs
	for _, n := range mockNisd {
		resp, err := c.PutNisd(&n)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	// GET all NISDs
	res, err := c.GetNisds(cpLib.GetReq{})
	assert.NoError(t, err)
	assert.Equal(t, len(racks), len(resp), "Expected %d racks but got %d", len(racks), len(resp))

	log.Infof("All %d racks validated successfully", len(racks))
	assert.NotEmpty(t, res)

	// Store results in the map
	for _, n := range res {
		Nisds[n.ID] = n
	}

	// Validation: NISD ↔ Hypervisor
	for _, n := range Nisds {
		var hvExists bool
		for _, hv := range Hypervisors {
			if hv.Name == n.HyperVisorID {
				hvExists = true
				break
			}
		}
		assert.True(t, hvExists,
			"NISD %s references HyperVisorID %s which does not exist among known Hypervisors",
			n.ID, n.HyperVisorID)
	}

	// Validation: NISD ↔ Device
	for _, n := range Nisds {
		var devExists bool
		for _, d := range Devices {
			if d.Name == n.DevID {
				devExists = true
				break
			}
		}
		assert.True(t, devExists,
			"NISD %s references DevID %s which does not exist among known Devices",
			n.ID, n.DevID)
	}

	// Validate count
	assert.Equal(t, len(mockNisd), len(res), "Mismatch in NISD count")

	log.Infof("Inserted %d NISDs, retrieved %d successfully.", len(mockNisd), len(res))

	// Validation: ensure inserted hypervisors are present with correct fields
	for _, expected := range hypervisors {
		found := false
		for _, got := range resp {
			if got.ID == expected.ID {
				assert.Equal(t, expected.Name, got.Name, "Mismatch in Name for ID %s", expected.ID)
				assert.Equal(t, expected.RackID, got.RackID, "Mismatch in RackID for ID %s", expected.ID)
				assert.Equal(t, expected.IPAddress, got.IPAddress, "Mismatch in IPAddress for ID %s", expected.ID)
				assert.Equal(t, expected.PortRange, got.PortRange, "Mismatch in PortRange for ID %s", expected.ID)
				assert.Equal(t, expected.SSHPort, got.SSHPort, "Mismatch in SSHPort for ID %s", expected.ID)
				found = true
				break
			}
		}
		assert.True(t, found, "Expected hypervisor with ID %s not found in response", expected.ID)
	}
}

func TestMultiCreateVdev(t *testing.T) {
	c := newClient(t)

	// Step 0: Create a NISD to allocate space for Vdevs
	n := cpLib.Nisd{
		ClientPort:    7001,
		PeerPort:      8001,
		ID:            "nisd-001",
		DevID:         "dev-001",
		HyperVisorID:  "hv-01",
		FailureDomain: "fd-01",
		IPAddr:        "192.168.1.10",
		InitDev:       true,
		TotalSize:     15_000_000_000_000, 
		AvailableSize: 15_000_000_000_000, 
	}
	_, err := c.PutNisd(&n)
	assert.NoError(t, err)

	// Step 1: Create first Vdev
	vdev1 := &cpLib.Vdev{
		Cfg: cpLib.VdevCfg{
			Size: 700 * 1024 * 1024 * 1024,
		}}
	resp, err := c.CreateVdev(vdev1)
	assert.NoError(t, err, "failed to create vdev1")
	assert.NotEmpty(t, resp.ID, "vdev1 ID should not be empty")
	log.Info("Created vdev1: ", resp.ID)

	// Step 2: Create second Vdev
	vdev2 := &cpLib.Vdev{
		Size: 400 * 1024 * 1024 * 1024}        // 400 GB
	err = c.CreateVdev(vdev2)
	assert.NoError(t, err)
	assert.Greater(t, vdev2.Size, int64(0), "Vdev size must be greater than 0")
	log.Info("CreateMultiVdev Result 2: ", vdev2)

	vdev3 := &cpLib.Vdev{
		Size: 800 * 1024 * 1024 * 1024}        // 800 GB
	err = c.CreateVdev(vdev3)
	assert.NoError(t, err)
	assert.Greater(t, vdev3.Size, int64(0), "Vdev size must be greater than 0")
	log.Info("CreateMultiVdev Result 3: ", vdev3)

	// Verify that IDs are unique
	assert.NotEqual(t, vdev1.VdevID, vdev2.VdevID, "Vdev IDs must be unique")

	// Verify that IDs are unique
	assert.NotEqual(t, vdev1.VdevID, vdev2.VdevID, "Vdev IDs must be unique")

	// Step 3: Fetch all Vdevs and validate both exist
	// Step 3: Fetch all Vdevs and validate both exist
	getAllReq := &cpLib.GetReq{GetAll: true}
	allCResp, err := c.GetVdevsWithChunkInfo(getAllReq)
	assert.NoError(t, err, "failed to fetch all vdevs with chunk mapping")
	assert.NotNil(t, allCResp, "all vdevs response with chunk mapping should not be nil")
	log.Info("All vdevs with chunk mapping response: ", allCResp)

	// Step 4: Fetch specific Vdev (vdev1)
	getSpecificReq := &cpLib.GetReq{
		ID:     vdev1.Cfg.ID,
		GetAll: false,
	}
	specificResp, err := c.GetVdevs(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev")
	assert.NotNil(t, specificResp, "specific vdev response should not be nil")
	log.Info("Specific vdev response: ", specificResp)

	assert.Equal(t, 1, len(specificResp), "expected exactly one vdev in specific fetch")
	assert.Equal(t, vdev1.VdevID, specificResp[0].VdevID, "fetched vdev ID mismatch")
	assert.Equal(t, vdev1.Size, specificResp[0].Size, "fetched vdev size mismatch")

	specificResp, err = c.GetVdevsWithChunkInfo(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev with chunk mapping")
	assert.NotNil(t, specificResp, "specific vdev with chunk mapping response should not be nil")
	log.Info("Specific vdev with chunk mapping response: ", specificResp)

	assert.Equal(t, 1, len(specificResp), "expected exactly one vdev with chunk mapping in specific fetch")
	assert.Equal(t, vdev1.VdevID, specificResp[0].VdevID, "fetched vdev ID mismatch")
	assert.Equal(t, vdev1.Size, specificResp[0].Size, "fetched vdev size mismatch")

	v := allCResp[0]
	log.Info("Validating Vdev from allCResp: ", v)

	// Validate internal structure and cross-object references
	for _, v := range []*cpLib.Vdev{vdev1, vdev2} {
		assert.NotEmpty(t, v.NisdToChkMap, "Each Vdev must have at least one NISD")
	
		for _, chunk := range v.NisdToChkMap {
			n := chunk.Nisd
			var nisdExists bool
			// Cross-validate 
			for _, nisd := range Nisds {
				if n.ID == nisd.ID {
					nisdExists = true
					log.Info("Vdev", v.VdevID, "references Nisd", n.ID, "which exists among known Nisds")
					// GET nisd 
					res, err := c.GetNisds(cpLib.GetReq{ID: n.ID})
					assert.NoError(t, err)
					assert.NotEmpty(t, res)
					returnedNisd := res[0]
					log.Info("Available Capacity of Nisd", nisd.ID, ": ", returnedNisd.AvailableSize)
					// ensure available capacity decreased after vdev creation
					assert.Lessf(t, returnedNisd.AvailableSize, nisd.AvailableSize,
						"Nisd %s AvailableSize (%d) did not decrease after vdev creation; original was %d",
						n.ID, returnedNisd.AvailableSize, nisd.AvailableSize)
					break
				}
			}
			assert.True(t, nisdExists, "Vdev %s references Nisd %s which does not exist among known Nisds", v.VdevID, n.ID)

			assert.NotEmpty(t, chunk.Chunk, "Chunk list for NISD %s must not be empty", n.ID)
		}
	}
}

func TestGetAllVdev(t *testing.T) {
	c := newClient(t)
	req := &cpLib.GetReq{
		GetAll: true,
	}
	resp, err := c.GetVdevs(req)
	log.Info("fetch all vdevs response: ", resp)
	assert.NoError(t, err)

	found := false
	for _, v := range resp {
		// Structural validation
		assert.NotEmpty(t, v.VdevID, "Each Vdev must have an ID")
		assert.Greater(t, v.Size, int64(0), "Vdev size must be valid")
		assert.NotEmpty(t, v.NisdToChkMap, "Each Vdev must have at least one NISD mapping")

		if v.VdevID == VDEV_ID {
			found = true
		}

		// Cross-entity validation
		for _,  chunk:= range v.NisdToChkMap {
			n := chunk.Nisd
			var nisdExists bool

			for _, nisd := range Nisds {
				if n.ID == nisd.ID {
					nisdExists = true
					log.Info("Vdev", v.VdevID, "references Nisd", n.ID, "which exists among known Nisds")
					break
				}
			}
			assert.True(t, nisdExists,"Vdev %s references Nisd %s which does not exist among known Nisds", v.VdevID, n.ID)
		}
	}

	assert.True(t, found, "Expected VDEV_ID %s to be returned in GetAllVdev", VDEV_ID)
}

	for _, chunk := range v.NisdToChkMap {
		n := chunk.Nisd
		var nisdExists bool
		assert.NotEmpty(t, n.ID, "NISD ID should not be empty")
		assert.NotEmpty(t, n.DevID, "Device ID should not be empty")
		assert.NotEmpty(t, n.HyperVisorID, "Hypervisor ID should not be empty")

		for _, nisd := range Nisds {
			if n.ID == nisd.ID {
				nisdExists = true
				log.Info("Vdev", v.VdevID, "references Nisd", n.ID, "which exists among known Nisds")
				break
			}
		}
		assert.True(t, nisdExists,"Vdev %s references Nisd %s which does not exist among known Nisds", v.VdevID, n.ID)
	}
}

func TestPutAndGetSinglePartition(t *testing.T) {
	c := newClient(t)

	pt := &cpLib.DevicePartition{
		PartitionID:   "nvme-Amazon_Elastic_Block_Store_vol0dce303259b3884dc-part1",
		DevID:         "nvme-Amazon_Elastic_Block_Store_vol0dce303259b3884dc",
		Size:          10 * 1024 * 1024 * 1024,
		PartitionPath: "some path",
		NISDUUID:      "b962cea8-ab42-11f0-a0ad-1bd216770b60",
	}

	// Put partition
	resp, err := c.PutPartition(pt)
	assert.NoError(t, err, "Error while putting partition")
	assert.True(t, resp.Success, "PutPartition response not successful")

	// Get partition by ID
	resp1, err := c.GetPartition(cpLib.GetReq{ID: pt.PartitionID})
	assert.NoError(t, err, "Error while getting partition by ID")
	assert.Equal(t, 1, len(resp1), "Expected exactly one partition in Get response")

	returned := resp1[0]

	// Validate the retrieved partition details
	assert.Equal(t, pt.PartitionID, returned.PartitionID, "Mismatch in PartitionID")
	assert.Equal(t, pt.PartitionPath, returned.PartitionPath, "Mismatch in PartitionPath")
	assert.Equal(t, pt.NISDUUID, returned.NISDUUID, "Mismatch in NISDUUID")
	assert.Equal(t, pt.DevID, returned.DevID, "Mismatch in DevID")
	assert.Equal(t, pt.Size, returned.Size, "Mismatch in Size (bytes)")

	log.Infof("Single Partition PUT/GET validation successful for Partition ID: %s", pt.PartitionID)
	log.Infof("Single Partition PUT/GET validation successful for Partition ID: %s", pt.PartitionID)
}

func runPutAndGetRack(b testing.TB, c *CliCFuncs) {
	racks := []cpLib.Rack{
		{ID: "9bc244bc-df29-11f0-a93b-277aec17e437", PDUID: "95f62aee-997e-11f0-9f1b-a70cff4b660b"},
		{ID: "3704e442-df2b-11f0-be6a-776bc1500ab8", PDUID: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea"},
	}

	for _, r := range racks {
		resp, err := c.PutRack(&r)
		assert.NoError(b, err)
		assert.True(b, resp.Success)
	}

	resp, err := c.GetRacks(&cpLib.GetReq{GetAll: true})
	assert.NoError(b, err)
	_ = resp
}

func BenchmarkPutAndGetRack(b *testing.B) {
	c := newClient(nil) // adjust if your newClient requires *testing.T
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runPutAndGetRack(b, c)
	}
}

func TestVdevNisdChunk(t *testing.T) {

	c := newClient(t)

	// create nisd
	mockNisd := cpLib.Nisd{
		ClientPort: 7001,
		PeerPort:   8001,
		ID:         "nisd-001",
		FailureDomain: []string{
			"pdu-05",
			"rack-04",
			"hv-07",
			"dev-004",
		},
		IPAddr:        "192.168.1.10",
		TotalSize:     1_000_000_000_000, // 1 TB
		AvailableSize: 750_000_000_000,   // 750 GB
	}
	resp, err := c.PutNisd(&mockNisd)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// create vdev
	vdev := &cpLib.Vdev{
		Cfg: cpLib.VdevCfg{
			Size: 500 * 1024 * 1024 * 1024,
		}}
	err = c.CreateVdev(vdev)
	log.Info("Created Vdev Result: ", vdev)
	assert.NoError(t, err)
	readV, err := c.GetVdevCfg(&cpLib.GetReq{ID: vdev.Cfg.ID})
	log.Info("Read vdev:", readV)
	nc, err := c.GetChunkNisd(&cpLib.GetReq{ID: path.Join(vdev.Cfg.ID, "2")})
	log.Info("Read Nisd Chunk:", nc)
}

func TestPutAndGetNisdArgs(t *testing.T) {
	c := newClient(t)
	na := &cpLib.NisdArgs{
		Defrag:               true,
		MBCCnt:               8,
		MergeHCnt:            4,
		MCIBReadCache:        256,
		S3:                   "s3://backup-bucket/data",
		DSync:                "enabled",
		AllowDefragMCIBCache: false,
	}
	resp, err := c.PutNisdArgs(na)
	log.Info("created na: ", resp)
	assert.NoError(t, err)
	nisdArgs, err := c.GetNisdArgs()
	assert.NoError(t, err)
	log.Info("Get na: ", nisdArgs)
}
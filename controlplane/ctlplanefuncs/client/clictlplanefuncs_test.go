package clictlplanefuncs

import (
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
    "time"

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
	TestNisds = make(map[string]cpLib.Nisd)
	TestNisdsAfter = make(map[string]cpLib.Nisd)
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

	pdu := cpLib.PDU{
		ID: "95f62aee-997e-11f0-9f1b-a70cff4b660b",
		Name:          "pdu-1",
		Location:      "us-east",
		PowerCapacity: "15Kw",
		Specification: "specification1",
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

	// PUT multiple Racks
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
		RackID:     "8a5303ae-ab23-11f0-bb87-632ad3e09c04",
		ID:         "89944570-ab2a-11f0-b55d-8fc2c05d35f4",
		IPAddrs:  	[]string{"127.0.0.1", "127.0.0.1"},
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
	assert.Equal(t, hv.PortRange, getResp[0].PortRange)
	assert.Equal(t, hv.SSHPort, getResp[0].SSHPort)

	log.Infof("Single Hypervisor PUT/GET validation successful for Hypervisor ID: %s", hv.ID)	
}

func TestPutAndGetMultipleHypervisors(t *testing.T) {
	c := newClient(t)

	hypervisors := []cpLib.Hypervisor{
		{RackID: "8a5303ae-ab23-11f0-bb87-632ad3e09c04", ID: "89944570-ab2a-11f0-b55d-8fc2c05d35f4", IPAddrs: []string{"127.0.0.1", "127.0.0.1"}, PortRange: "8000-9000", SSHPort: "6999", Name: "hv-1"},
		{RackID: "93e2925e-ab23-11f0-958d-87f55a6a9981", ID: "8f70f2a4-ab2a-11f0-a1bb-cb25e1fa6a6b", IPAddrs: []string{"127.0.0.1", "127.0.0.1"}, PortRange: "5000-7000", SSHPort: "7999", Name: "hv-2"},
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

	log.Infof("All %d hypervisors validated successfully", len(hypervisors))

	// Validation: ensure inserted hypervisors are present with correct fields
	for _, expected := range hypervisors {
		found := false
		for _, got := range resp {
			if got.ID == expected.ID {
				assert.Equal(t, expected.Name, got.Name, "Mismatch in Name for ID %s", expected.ID)
				assert.Equal(t, expected.RackID, got.RackID, "Mismatch in RackID for ID %s", expected.ID)
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
			if rack.ID == hv.RackID {
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
		ID:            "nvme-fb6358162001",
		SerialNumber:  "SN112233445",
		State:         2,
		HypervisorID:  "89944570-ab2a-11f0-b55d-8fc2c05d35f4",
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

	mockDevices := []cpLib.Device{
		{
			ID:            "nvme-fb6358162001",
			SerialNumber:  "SN123456789",
			State:         1,
			HypervisorID:  "89944570-ab2a-11f0-b55d-8fc2c05d35f4",
			FailureDomain: "fd-01",
			DevicePath:    "/temp/path1",
			Name:          "dev-1",
		},
		{
			ID:            "nvme-fb6358162002",
			SerialNumber:  "SN987654321",
			State:         0,
			HypervisorID:  "8f70f2a4-ab2a-11f0-a1bb-cb25e1fa6a6b",
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
				DevID:         "nvme-fb6358162002",
				Size:          123467,
			},
			},
		},
		{
			ID:            "nvme-fb6358162003",
			SerialNumber:  "SN112233445",
			State:         2,
			HypervisorID:  "89944570-ab2a-11f0-b55d-8fc2c05d35f4",
			FailureDomain: "fd-02",
			DevicePath:    "/temp/path3",
			Name:          "dev-3",
			Size:          9999999,
			Partitions: []cpLib.DevicePartition{cpLib.DevicePartition{
				PartitionID:   "b97c3464-ab3e-11f0-b32d-9775558a141a",
				PartitionPath: "/part/path3",
				NISDUUID:      "1",
				DevID:         "nvme-fb6358162003",
				Size:          123467,
			},
			},
		},
	}

	// PUT multiple devices
	for _, d := range mockDevices {
		resp, err := c.PutDevice(&d)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	// GET all devices
	res, err := c.GetDevices(cpLib.GetReq{})
	log.Infof("fetch single device info: %s, %s, %s", res[0].ID, res[0].HypervisorID, res[0].SerialNumber)
	assert.NoError(t, err)

	// Store results in the map
	for _, d := range res {
		Devices[d.ID] = d
	}

	// Validation: Device ↔ Hypervisor
	for _, device := range Devices {
		var hvExists bool
		for _, hv := range Hypervisors {
			if hv.ID == device.HypervisorID {
				hvExists = true
				break
			}
		}
		assert.True(t, hvExists, "Device %s references HypervisorID %s which does not exist", device.Name, device.HypervisorID)
	}

	// Validate count
	assert.Equal(t, len(mockDevices), len(res), "Mismatch in number of devices retrieved")

	log.Infof("Inserted %d devices, retrieved %d successfully", len(mockDevices), len(res))
}

func TestPutAndGetSingleNisd(t *testing.T) {
   c := newClient(t)

   nisd := cpLib.Nisd{
           PeerPort:      8001,
           ID:            "3355858e-ea0e-11f0-9768-e71fc352cd1d",
           FailureDomain: []string{
               "95f62aee-997e-11f0-9f1b-a70cff4b660b",
               "8a5303ae-ab23-11f0-bb87-632ad3e09c04",
               "89944570-ab2a-11f0-b55d-8fc2c05d35f4",
               "nvme-fb6358162001",
           },
		   TotalSize:     1_000_000_000_000, // 1 TB
           AvailableSize: 750_000_000_000,   // 750 GB
           SocketPath:    "/path/sockets1",
		   NetInfo: cpLib.NetInfoList{
			 	cpLib.NetworkInfo{
					IPAddr: "192.168.0.0.1",
					Port:   5444,
				},
				cpLib.NetworkInfo{
					IPAddr: "192.168.0.0.2",
					Port:   6444,
				},
		   },
		   NetInfoCnt: 2,
   }

   // PUT operation
   resp, err := c.PutNisd(&nisd)
   assert.NoError(t, err)
   assert.True(t, resp.Success)

   // GET operation
   res, err := c.GetNisds()
   log.Info("GetNisd: ", res)
   assert.NoError(t, err)
   assert.NotEmpty(t, res)
  
   returned := res[0]

   // Validate all key fields match
   assert.Equal(t, nisd.ID, returned.ID, "ID mismatch")
   assert.Equal(t, nisd.FailureDomain, returned.FailureDomain, "FailureDomain mismatch")
   assert.Equal(t, nisd.PeerPort, returned.PeerPort, "PeerPort mismatch")
   assert.Equal(t, nisd.TotalSize, returned.TotalSize, "TotalSize mismatch")
   assert.Equal(t, nisd.AvailableSize, returned.AvailableSize, "AvailableSize mismatch")

   log.Info("Single NISD PUT/GET validations successful.")
}

func TestPutAndGetMultipleNisds(t *testing.T) {
	c := newClient(t)

	mockNisd := []cpLib.Nisd{
		{
			PeerPort:   8002,
			ID:         "397fc08c-ea0e-11f0-9e9a-47f5587ee2a6",
			FailureDomain: []string{
				"13ce1c48-9979-11f0-8bd0-4f62ec9356ea",
				"93e2925e-ab23-11f0-958d-87f55a6a9981",
				"8f70f2a4-ab2a-11f0-a1bb-cb25e1fa6a6b",
				"nvme-fb6358162002",
			},
			TotalSize:     500_000_000_000, // 500 GB
			AvailableSize: 200_000_000_000, // 200 GB
			SocketPath:    "/path/sockets2",
			NetInfo: cpLib.NetInfoList{
				cpLib.NetworkInfo{
					IPAddr: "192.168.0.0.1",
					Port:   5444,
				},
				cpLib.NetworkInfo{
					IPAddr: "192.168.0.0.2",
					Port:   6444,
				},
			},
			NetInfoCnt: 2,
		},
		{
			PeerPort:   8003,
			ID:         "397fc08c-ea0e-11f0-9e9a-47f5587ee1f8",
			FailureDomain: []string{
				"13ce1c48-9979-11f0-8bd0-4f62ec9356ea",
				"8a5303ae-ab23-11f0-bb87-632ad3e09c04",
				"89944570-ab2a-11f0-b55d-8fc2c05d35f4",
				"nvme-fb6358162003",
			},
			TotalSize:     2_000_000_000_000, // 2 TB
			AvailableSize: 1_500_000_000_000, // 1.5 TB
			SocketPath:    "/path/sockets3",
			NetInfo: cpLib.NetInfoList{
				cpLib.NetworkInfo{
					IPAddr: "192.168.0.0.1",
					Port:   5444,
				},
				cpLib.NetworkInfo{
					IPAddr: "192.168.0.0.2",
					Port:   6444,
				},
			},
			NetInfoCnt: 2,
		},
	}

	// PUT multiple NISDs 
	for _, n := range mockNisd {
        resp, err := c.PutNisd(&n)
        assert.NoError(t, err)
        assert.True(t, resp.Success)
        log.Info("NISD created: ", n.ID)
    }

	// GET all NISDs
	res, err := c.GetNisds()
	assert.NoError(t, err)
	assert.NotEmpty(t, res)

	// Store results in the map
	for _, n := range res {
		Nisds[n.ID] = n
	}

	// Validate count
	assert.Equal(t, len(mockNisd), len(res), "Mismatch in NISD count")

	log.Infof("Inserted %d NISDs, retrieved %d successfully.", len(mockNisd), len(res))
}

func TestMultiCreateVdev(t *testing.T) {
	c := newClient(t)

	// Step 0: Create a NISD to allocate space for Vdevs
	n := cpLib.Nisd{
		PeerPort: 8001,
		ID:       "3355858e-ea0e-11f0-9768-e71fc352cd1d",
		FailureDomain: []string{
			"95f62aee-997e-11f0-9f1b-a70cff4b660b",
			"8a5303ae-ab23-11f0-bb87-632ad3e09c04",
			"89944570-ab2a-11f0-b55d-8fc2c05d35f4",
			"nvme-fb6358162001",
		},
		TotalSize:     15_000_000_000_000, // 1 TB
		AvailableSize: 15_000_000_000_000, // 750 GB
		SocketPath:    "/path/sockets1",
		NetInfo: cpLib.NetInfoList{
			 	cpLib.NetworkInfo{
					IPAddr: "192.168.0.0.1",
					Port:   5444,
				},
				cpLib.NetworkInfo{
					IPAddr: "192.168.0.0.2",
					Port:   6444,
				},
		    },
		NetInfoCnt: 2,
	}
	_, err := c.PutNisd(&n)
	assert.NoError(t, err)

	// Step 1: Create first Vdev
	req1 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			Size:       500 * 1024 * 1024 * 1024,
			NumReplica: 1,
		},
	}
	resp, err := c.CreateVdev(req1)
	assert.NoError(t, err, "failed to create vdev1")
	assert.NotEmpty(t, resp, "vdev1 ID should not be empty")
	log.Info("Created vdev1: ", resp.ID)

	// Step 2: Create second Vdev
	req2 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			Size:       500 * 1024 * 1024 * 1024,
			NumReplica: 1,
		},
	}
	resp, err = c.CreateVdev(req2)
	assert.NoError(t, err, "failed to create vdev2")
	assert.NotEmpty(t, resp, "vdev2 ID should not be empty")
	log.Info("Created vdev2: ", resp)

	// Step 3: Fetch all Vdevs and validate both exist
	getAllReq := &cpLib.GetReq{GetAll: true}

	allCResp, err := c.GetVdevsWithChunkInfo(getAllReq)
	assert.NoError(t, err, "failed to fetch all vdevs with chunk mapping")
	assert.NotNil(t, allCResp, "all vdevs response with chunk mapping should not be nil")
	log.Info("All vdevs with chunk mapping response: ", allCResp)

	// Step 4: Fetch specific Vdev (vdev1)
	getSpecificReq := &cpLib.GetReq{
		ID:     req1.Vdev.ID,
		GetAll: false,
	}
	specificResp, err := c.GetVdevCfg(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev")
	assert.NotNil(t, specificResp, "specific vdev response should not be nil")
	log.Info("Specific vdev response: ", specificResp)

	assert.Equal(t, req1.Vdev.ID, specificResp.ID, "fetched vdev ID mismatch")
	assert.Equal(t, req1.Vdev.Size, specificResp.Size, "fetched vdev size mismatch")

	result, err := c.GetVdevsWithChunkInfo(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev with chunk mapping")
	assert.NotNil(t, result, "specific vdev with chunk mapping response should not be nil")
	log.Info("Specific vdev with chunk mapping response: ", result)

	assert.Equal(t, 1, len(result), "expected exactly one vdev with chunk mapping in specific fetch")
	assert.Equal(t, vdev1.Cfg.ID, result[0].Cfg.ID, "fetched vdev ID mismatch")
	assert.Equal(t, vdev1.Cfg.Size, result[0].Cfg.Size, "fetched vdev size mismatch")

	assert.NotEqual(t, vdev1.Cfg.ID, vdev2.Cfg.ID, "Vdev IDs must be unique")

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
					log.Info("Vdev", v.Cfg.ID, "references Nisd", n.ID, "which exists among known Nisds")
					// GET nisd 
					res, err := c.GetNisd(cpLib.GetReq{ID: n.ID})
					assert.NoError(t, err)
					assert.NotEmpty(t, res)
					returnedNisd := res
					log.Info("Available Capacity of Nisd", nisd.ID, ": ", returnedNisd.AvailableSize)
					// ensure available capacity decreased after vdev creation
					assert.Lessf(t, returnedNisd.AvailableSize, nisd.AvailableSize,
						"Nisd %s AvailableSize (%d) did not decrease after vdev creation; original was %d",
						n.ID, returnedNisd.AvailableSize, nisd.AvailableSize)
					break
				}
			}
			assert.True(t, nisdExists, "Vdev %s references Nisd %s which does not exist among known Nisds", v.Cfg.ID, n.ID)

			assert.NotEmpty(t, chunk.Chunk, "Chunk list for NISD %s must not be empty", n.ID)
		}
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
}

func runPutAndGetRack(b testing.TB, c *CliCFuncs) {
	racks := []cpLib.Rack{
		{ID: "9bc244bc-df29-11f0-a93b-277aec17e437", PDUID: "95f62aee-997e-11f0-9f1b-a70cff4b660b"},
		{ID: "3704e442-df2b-11f0-be6a-776bc1500ab8", PDUID: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea"},
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
		PeerPort: 8001,
		ID:       "1d67328a-df29-11f0-9e36-d7e439f8e740",
		FailureDomain: []string{
			"17ab4598-df29-11f0-afa1-2f5633c6b6c9",
			"2435b29e-df29-11f0-900b-d3d680074046",
			"298cedc0-df29-11f0-8c85-e3df2426ed67",
			"nvme-e3df2426ed67",
		},
		TotalSize:     1_000_000_000_000, // 1 TB
		AvailableSize: 750_000_000_000,   // 750 GB
		SocketPath:    "/path/sockets1",
		NetInfo: cpLib.NetInfoList{
			 	cpLib.NetworkInfo{
					IPAddr: "192.168.0.0.1",
					Port:   5444,
				},
				cpLib.NetworkInfo{
					IPAddr: "192.168.0.0.2",
					Port:   6444,
				},
		},
		NetInfoCnt: 2,
	}
	resp, err := c.PutNisd(&mockNisd)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// create vdev
	vdev := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			Size:       500 * 1024 * 1024 * 1024,
			NumReplica: 1,
		},
	}
	resp, err = c.CreateVdev(vdev)
	log.Info("Created Vdev Result: ", resp)
	assert.NoError(t, err)
	readV, err := c.GetVdevCfg(&cpLib.GetReq{ID: resp.ID})
	log.Info("Read vdev:", readV)
	nc, _ := c.GetChunkNisd(&cpLib.GetReq{ID: path.Join("019b01bf-fd55-7e56-9f30-0005860e36a9", "2")})
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

func TestHierarchy(t *testing.T) {
	c := newClient(t)

	pdus := []string{
		"9bc244bc-df29-11f0-a93b-277aec17e401",
		"9bc244bc-df29-11f0-a93b-277aec17e402",
		"9bc244bc-df29-11f0-a93b-277aec17e403",
		"9bc244bc-df29-11f0-a93b-277aec17e404",
		"9bc244bc-df29-11f0-a93b-277aec17e405",
	}

	// 10 RACKS
	racks := []string{
		"3f082930-df29-11f0-ab7b-4bd430991101",
		"3f082930-df29-11f0-ab7b-4bd430991102",
		"3f082930-df29-11f0-ab7b-4bd430991103",
		"3f082930-df29-11f0-ab7b-4bd430991104",
		"3f082930-df29-11f0-ab7b-4bd430991105",
		"3f082930-df29-11f0-ab7b-4bd430991106",
		"3f082930-df29-11f0-ab7b-4bd430991107",
		"3f082930-df29-11f0-ab7b-4bd430991108",
		"3f082930-df29-11f0-ab7b-4bd430991109",
		"3f082930-df29-11f0-ab7b-4bd430991110",
	}

	// 20 HVs
	hvs := []string{
		"bde1f08a-df63-11f0-88ef-430ddec19901",
		"bde1f08a-df63-11f0-88ef-430ddec19902",
		"bde1f08a-df63-11f0-88ef-430ddec19903",
		"bde1f08a-df63-11f0-88ef-430ddec19904",
		"bde1f08a-df63-11f0-88ef-430ddec19905",
		"bde1f08a-df63-11f0-88ef-430ddec19906",
		"bde1f08a-df63-11f0-88ef-430ddec19907",
		"bde1f08a-df63-11f0-88ef-430ddec19908",
		"bde1f08a-df63-11f0-88ef-430ddec19909",
		"bde1f08a-df63-11f0-88ef-430ddec19910",
		"bde1f08a-df63-11f0-88ef-430ddec19911",
		"bde1f08a-df63-11f0-88ef-430ddec19912",
		"bde1f08a-df63-11f0-88ef-430ddec19913",
		"bde1f08a-df63-11f0-88ef-430ddec19914",
		"bde1f08a-df63-11f0-88ef-430ddec19915",
		"bde1f08a-df63-11f0-88ef-430ddec19916",
		"bde1f08a-df63-11f0-88ef-430ddec19917",
		"bde1f08a-df63-11f0-88ef-430ddec19918",
		"bde1f08a-df63-11f0-88ef-430ddec19919",
		"bde1f08a-df63-11f0-88ef-430ddec19920",
	}

	// 40 Devices
	devices := []string{
		"nvme-fb6358163001",
		"nvme-fb6358163002",
		"nvme-fb6358163003",
		"nvme-fb6358163004",
		"nvme-fb6358163005",
		"nvme-fb6358163006",
		"nvme-fb6358163007",
		"nvme-fb6358163008",
		"nvme-fb6358163009",
		"nvme-fb6358163010",
		"nvme-fb6358163011",
		"nvme-fb6358163012",
		"nvme-fb6358163013",
		"nvme-fb6358163014",
		"nvme-fb6358163015",
		"nvme-fb6358163016",
		"nvme-fb6358163017",
		"nvme-fb6358163018",
		"nvme-fb6358163019",
		"nvme-fb6358163020",
		"nvme-fb6358163021",
		"nvme-fb6358163022",
		"nvme-fb6358163023",
		"nvme-fb6358163024",
		"nvme-fb6358163025",
		"nvme-fb6358163026",
		"nvme-fb6358163027",
		"nvme-fb6358163028",
		"nvme-fb6358163029",
		"nvme-fb6358163030",
		"nvme-fb6358163031",
		"nvme-fb6358163032",
		"nvme-fb6358163033",
		"nvme-fb6358163034",
		"nvme-fb6358163035",
		"nvme-fb6358163036",
		"nvme-fb6358163037",
		"nvme-fb6358163038",
		"nvme-fb6358163039",
		"nvme-fb6358163040",
	}

	mockNisd := make([]cpLib.Nisd, 0, 160)

	pduCount := len(pdus)
	rackPerPdu := 2
	hvPerRack := 2
	devPerHv := 2
	nisdPerDev := 4

	rackIdx := 0
	hvIdx := 0
	devIdx := 0

	nisdID := 1

	for p := 0; p < pduCount; p++ {
		pdu := pdus[p]

		for r := 0; r < rackPerPdu; r++ {
			rack := racks[rackIdx]
			rackIdx++

			for h := 0; h < hvPerRack; h++ {
				hv := hvs[hvIdx]
				hvIdx++

				for d := 0; d < devPerHv; d++ {
					dev := devices[devIdx]
					devIdx++

					for n := 0; n < nisdPerDev; n++ {
						nisd := cpLib.Nisd{
							PeerPort: 8000 + uint16(nisdID),
							ID:       fmt.Sprintf("ed7914c3-2e96-4f3e-8e0d-%012x", nisdID),
							FailureDomain: []string{
								pdu,
								rack,
								hv,
								dev,
							},
							TotalSize:     1_000_000_000_000,
							AvailableSize: 1_000_000_000_000,
						}

						mockNisd = append(mockNisd, nisd)
						nisdID++
					}
				}
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for _, n := range mockNisd {
			resp, err := c.PutNisd(&n)
			assert.NoError(t, err)
			assert.True(t, resp.Success)
			time.Sleep(10 * time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 150; i++ {
			vdev := &cpLib.VdevReq{
				Vdev: &cpLib.VdevCfg{
					Size:       100 * 1024 * 1024 * 1024,
					NumReplica: 1,
				},
			}
			resp, err := c.CreateVdev(vdev)
			log.Info("succesfully created vdev: ", resp)
			assert.NoError(t, err)
			time.Sleep(30 * time.Millisecond)
		}
	}()

	wg.Wait()

	// -------------------------------
	// NISD Distribution Verification
	// -------------------------------

	// GET all NISDs
	res, err := c.GetNisds()
	assert.NoError(t, err)
	assert.NotEmpty(t, res)

	// Store results in the map
	for _, n := range res {
		TestNisds[n.ID] = n
	}

	// Validate count
	assert.Equal(t, len(mockNisd), len(res), "Mismatch in NISD count")

	var usedSizes []int64
	var totalUsed int64

	for _, n := range TestNisds {
		used := n.TotalSize - n.AvailableSize
		usedSizes = append(usedSizes, used)
		totalUsed += used
	}

	// Compute average usage
	avgUsed := totalUsed / int64(len(usedSizes))

	// Allow small skew due to placement constraints (5%)
	allowedSkew := avgUsed / 20

	for i, used := range usedSizes {
		diff := int64(used) - int64(avgUsed)
		if diff < 0 {
			diff = -diff
		}

		assert.LessOrEqualf(
			t,
			uint64(diff),
			allowedSkew,
			"NISD %d usage imbalance detected: used=%d avg=%d",
			i,
			used,
			avgUsed,
		)
	}

	// -------------------------------
	// NISD Exhaustion Test
	// -------------------------------

	// Try allocating until exhaustion
	var exhaustionErr error

	for i := 0; i < 10_000; i++ {
		vdev := &cpLib.Vdev{
			Cfg: cpLib.VdevCfg{
				Size:       50 * 1024 * 1024 * 1024, // 50GB chunks
				NumReplica: 3, // Fault tolerance enabled
			},
		}

		_, err := c.CreateVdev(vdev)
		if err != nil {
			exhaustionErr = err
			break
		}
	}

	assert.Error(t, exhaustionErr)
	assert.Contains(
		t,
		exhaustionErr.Error(),
		"Not enough space",
	)

	// Verify some space still remains (cannot hit 100%)
	// GET all NISDs
	nisdsAfter, err := c.GetNisds()

	// Store results in the map
	for _, n := range nisdsAfter {
		TestNisdsAfter[n.ID] = n
	}

	var remaining int64
	for _, n := range TestNisdsAfter {
		remaining += n.AvailableSize
	}

	assert.Greater(
		t,
		remaining,
		int64(0),
		"Expected remaining space due to fault tolerance constraints",
	)
}

func TestCreateSmallHierarchy(t *testing.T) {
	c := newClient(t)

	pdus := []string{
		"9bc244bc-df29-11f0-a93b-277aec17e43701",
		"9bc244bc-df29-11f0-a93b-277aec17e43702",
	}

	// 2 RACKS
	racks := []string{
		"3f082930-df29-11f0-ab7b-4bd430991101",
		"3f082930-df29-11f0-ab7b-4bd430991102",
	}

	// 5 HVs
	hvs := []string{
		"bde1f08a-df63-11f0-88ef-430ddec199701",
		"bde1f08a-df63-11f0-88ef-430ddec199702",
		"bde1f08a-df63-11f0-88ef-430ddec199703",
		"bde1f08a-df63-11f0-88ef-430ddec199704",
		"bde1f08a-df63-11f0-88ef-430ddec199705",
	}

	// 6 Devices
	devices := []string{
		"nvme-fb6358163001",
		"nvme-fb6358163002",
		"nvme-fb6358163003",
		"nvme-fb6358163004",
		"nvme-fb6358163005",
		"nvme-fb6358163006",
	}

	mockNisd := []cpLib.Nisd{
		cpLib.Nisd{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5661",
			FailureDomain: []string{
				pdus[0],
				racks[0],
				hvs[0],
				devices[0],
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		cpLib.Nisd{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5662",
			FailureDomain: []string{
				pdus[0],
				racks[0],
				hvs[0],
				devices[1],
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		cpLib.Nisd{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5663",
			FailureDomain: []string{
				pdus[0],
				racks[0],
				hvs[1],
				devices[2],
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		cpLib.Nisd{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5664",
			FailureDomain: []string{
				pdus[1],
				racks[1],
				hvs[2],
				devices[3],
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		cpLib.Nisd{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5665",
			FailureDomain: []string{
				pdus[1],
				racks[1],
				hvs[3],
				devices[4],
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		cpLib.Nisd{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5666",
			FailureDomain: []string{
				pdus[1],
				racks[1],
				hvs[4],
				devices[5],
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
	}

	for _, n := range mockNisd {
		resp, err := c.PutNisd(&n)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	// Total available space from mockNisd = 6 * 1TB
	totalAvailable := int64(6 * 1073741824000)

	// Request more than total available space
	requestedVdevSize := totalAvailable + 107374182400 // +100GB

	vdev := &cpLib.Vdev{
		Cfg: cpLib.VdevCfg{
			Size:       requestedVdevSize,
			NumReplica: 3,
func TestCreateVdev(t *testing.T) {
	c := newClient(t)
	vdev := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			Size:       500 * 1024 * 1024 * 1024,
			NumReplica: 2,
		},
		Filter: cpLib.Filter{
			Type: cpLib.FD_HV,
		},
	}

	resp, err := c.CreateVdev(vdev)
	assert.NoError(t, err)

	// Expect an error
	log.Infof("vdev1 response status: %v", resp)
}

func TestCreateVdev(t *testing.T) {
	c := newClient(t)

	nisd1 := cpLib.Nisd{
        PeerPort:      8001,
        ID:            "e3a6c2f1-9b7d-4a5e-8c42-1f0d6b9a7e55",
        FailureDomain: []string{
            "9bc244bc-df29-11f0-a93b-277aec17e401",
            "3f082930-df29-11f0-ab7b-4bd430991101",
            "bde1f08a-df63-11f0-88ef-430ddec19901",
            "nvme-fb6358163001",
        },
        TotalSize:     1_000_000_000_000, // 1 TB
        AvailableSize: 750_000_000_000,   // 750 GB
   }
   // PUT operation
   resp, err := c.PutNisd(&nisd1)
   assert.NoError(t, err)
   assert.True(t, resp.Success)

	vdev1 := &cpLib.Vdev{
		Cfg: cpLib.VdevCfg{
			Size:       1700 * 1024 * 1024 * 1024,
			NumReplica: 3,
		},
	}

	res, er := c.CreateVdev(vdev1)
	assert.NoError(t, er)
	log.Infof("vdev1 response status: %v", res)

	nisd2 := cpLib.Nisd{
        PeerPort:      8002,
        ID:            "e3a6c2f1-9b7d-4a5e-8c42-1f0d6b9a7e56",
        FailureDomain: []string{
            "9bc244bc-df29-11f0-a93b-277aec17e402",
            "3f082930-df29-11f0-ab7b-4bd430991102",
            "bde1f08a-df63-11f0-88ef-430ddec19902",
            "nvme-fb6358163002",
        },
        TotalSize:     1_000_000_000_000, // 1 TB
        AvailableSize: 750_000_000_000,   // 750 GB
   }
   // PUT operation
   resp1, err1 := c.PutNisd(&nisd2)
   assert.NoError(t, err1)
   assert.True(t, resp1.Success)

	vdev2 := &cpLib.Vdev{
		Cfg: cpLib.VdevCfg{
			Size:       1000 * 1024 * 1024 * 1024,
			NumReplica: 6,
		},
	}

	res1, er1 := c.CreateVdev(vdev2)
	assert.NoError(t, er1)
	log.Infof("vdev2 response status: %v", res1)

	nisd3 := cpLib.Nisd{
        PeerPort:      8003,
        ID:            "e3a6c2f1-9b7d-4a5e-8c42-1f0d6b9a7e57",
        FailureDomain: []string{
            "9bc244bc-df29-11f0-a93b-277aec17e403",
            "3f082930-df29-11f0-ab7b-4bd430991103",
            "bde1f08a-df63-11f0-88ef-430ddec19903",
            "nvme-fb6358163003",
        },
        TotalSize:     1_000_000_000_000, // 1 TB
        AvailableSize: 750_000_000_000,   // 750 GB
   }
   // PUT operation
   resp2, err2 := c.PutNisd(&nisd3)
   assert.NoError(t, err2)
   assert.True(t, resp2.Success)

	vdev3 := &cpLib.Vdev{
		Cfg: cpLib.VdevCfg{
			Size:       1200 * 1024 * 1024 * 1024,
			NumReplica: 15,
		},
	}

	res2, er2 := c.CreateVdev(vdev3)
	assert.NoError(t, er2)
	log.Infof("vdev3 response status: %v", res2)

	nisd4 := cpLib.Nisd{
        PeerPort:      8004,
        ID:            "e3a6c2f1-9b7d-4a5e-8c42-1f0d6b9a7e58",
        FailureDomain: []string{
            "9bc244bc-df29-11f0-a93b-277aec17e404",
            "3f082930-df29-11f0-ab7b-4bd430991104",
            "bde1f08a-df63-11f0-88ef-430ddec19904",
            "nvme-fb6358163004",
        },
        TotalSize:     1_000_000_000_000, // 1 TB
        AvailableSize: 750_000_000_000,   // 750 GB
   }
   // PUT operation
   resp3, err3 := c.PutNisd(&nisd4)
   assert.NoError(t, err3)
   assert.True(t, resp3.Success)

	vdev4 := &cpLib.Vdev{
		Cfg: cpLib.VdevCfg{
			Size:       1300 * 1024 * 1024 * 1024,
			NumReplica: 25,
		},
	}

	res3, er3 := c.CreateVdev(vdev4)
	assert.NoError(t, er3)
	log.Infof("vdev4 response status: %v", res3)

	nisd5 := cpLib.Nisd{
        PeerPort:      8005,
        ID:            "e3a6c2f1-9b7d-4a5e-8c42-1f0d6b9a7e59",
        FailureDomain: []string{
            "9bc244bc-df29-11f0-a93b-277aec17e405",
            "3f082930-df29-11f0-ab7b-4bd430991105",
            "bde1f08a-df63-11f0-88ef-430ddec19905",
            "nvme-fb6358163005",
        },
        TotalSize:     1_000_000_000_000, // 1 TB
        AvailableSize: 750_000_000_000,   // 750 GB
   }
   // PUT operation
   resp4, err4 := c.PutNisd(&nisd5)
   assert.NoError(t, err4)
   assert.True(t, resp4.Success)

	// Failure Test: High fault tolerance
	vdev5 := &cpLib.Vdev{
		Cfg: cpLib.VdevCfg{
			Size:       1000 * 1024 * 1024 * 1024,
			NumReplica: 7,
		},
	}

	// Should return an error
	res4, er4 := c.CreateVdev(vdev5)
	assert.NoError(t, er4)
	log.Infof("vdev5 response status: %v", res4)
}

func usagePercent(n cpLib.Nisd) int64 {
	used := n.TotalSize - n.AvailableSize
	return (used * 100) / n.TotalSize
}

func TestGetNisd(t *testing.T) {
	c := newClient(t)
	res, err := c.GetNisds()
	for _, n := range res {
		log.Infof("Nisd ID: %s, usage: %d", n.ID, usagePercent(n))
	}
	log.Info("total number of nisd's : ", len(res))
	assert.NoError(t, err)
}
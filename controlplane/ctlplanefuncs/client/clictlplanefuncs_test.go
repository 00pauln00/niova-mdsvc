package clictlplanefuncs

import (
	"os"
	"testing"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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

func TestPutAndGetNisd(t *testing.T) {
	c := newClient(t)

	mockNisd := []cpLib.Nisd{
		{
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
		},
		{
			ClientPort:    7002,
			PeerPort:      8002,
			ID:            "nisd-002",
			DevID:         "dev-002",
			HyperVisorID:  "hv-01",
			FailureDomain: "fd-02",
			IPAddr:        "192.168.1.11",
			InitDev:       false,
			TotalSize:     500_000_000_000, // 500 GB
			AvailableSize: 200_000_000_000, // 200 GB
		},
		{
			ClientPort:    7003,
			PeerPort:      8003,
			ID:            "nisd-003",
			DevID:         "dev-003",
			HyperVisorID:  "hv-02",
			FailureDomain: "fd-01",
			IPAddr:        "192.168.1.12",
			InitDev:       true,
			TotalSize:     2_000_000_000_000, // 2 TB
			AvailableSize: 1_500_000_000_000, // 1.5 TB
		},
	}

	for _, n := range mockNisd {
		resp, err := c.PutNisdCfg(&n)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	res, err := c.GetNisdCfg(cpLib.GetReq{ID: "nisd-002"})
	log.Info("GetNisdCfg: ", res)
	assert.NoError(t, err)
	
	expected := mockNisd[1]
	returned := res[0]

	// Validate all key fields match
	assert.Equal(t, expected.ID, returned.ID, "ID mismatch")
	assert.Equal(t, expected.DevID, returned.DevID, "DevID mismatch")
	assert.Equal(t, expected.HyperVisorID, returned.HyperVisorID, "HyperVisorID mismatch")
	assert.Equal(t, expected.FailureDomain, returned.FailureDomain, "FailureDomain mismatch")
	assert.Equal(t, expected.IPAddr, returned.IPAddr, "IPAddr mismatch")
	assert.Equal(t, expected.ClientPort, returned.ClientPort, "ClientPort mismatch")
	assert.Equal(t, expected.PeerPort, returned.PeerPort, "PeerPort mismatch")
	assert.Equal(t, expected.TotalSize, returned.TotalSize, "TotalSize mismatch")
	assert.Equal(t, expected.AvailableSize, returned.AvailableSize, "AvailableSize mismatch")
	assert.Equal(t, expected.InitDev, returned.InitDev, "InitDev mismatch")

	log.Info("All NISD PUT/GET validations successful.")
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
		resp, err := c.PutDeviceInfo(&p)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	res, err := c.GetDeviceInfo(cpLib.GetReq{ID: "60447cd0-ab3e-11f0-aa15-1f40dd976538"})
	log.Infof("fetch single device info: %s, %s, %s", res[0].ID, res[0].HypervisorID, res[0].SerialNumber)
	assert.NoError(t, err)

	returned := res[0]
	expected := mockDevices[2]

	// Validate single fetch matches inserted data
	assert.Equal(t, expected.ID, returned.ID)
	assert.Equal(t, expected.SerialNumber, returned.SerialNumber)
	assert.Equal(t, expected.HypervisorID, returned.HypervisorID)
	assert.Equal(t, expected.FailureDomain, returned.FailureDomain)
	assert.Equal(t, expected.DevicePath, returned.DevicePath)
	assert.Equal(t, expected.Name, returned.Name)
	assert.Equal(t, expected.Size, returned.Size)
	assert.Equal(t, len(expected.Partitions), len(returned.Partitions), "Mismatch in number of partitions")

	for i := range expected.Partitions {
		assert.Equal(t, expected.Partitions[i].PartitionID, returned.Partitions[i].PartitionID)
		assert.Equal(t, expected.Partitions[i].PartitionPath, returned.Partitions[i].PartitionPath)
		assert.Equal(t, expected.Partitions[i].NISDUUID, returned.Partitions[i].NISDUUID)
		assert.Equal(t, expected.Partitions[i].DevID, returned.Partitions[i].DevID)
		assert.Equal(t, expected.Partitions[i].Size, returned.Partitions[i].Size)
	}

	log.Infof("Validated single device fetch: %s (%s)", returned.ID, returned.SerialNumber)

	res, err = c.GetDeviceInfo(cpLib.GetReq{GetAll: true})
	log.Infof("fetch all device list: %s,%v", res[0].ID, res[0].Partitions)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(res), len(mockDevices), "Expected all inserted devices in GetAll")

	// Validate that each inserted device exists in the GetAll response
	for _, inserted := range mockDevices {
		found := false
		for _, fetched := range res {
			if fetched.ID == inserted.ID {
				found = true
				assert.Equal(t, inserted.SerialNumber, fetched.SerialNumber)
				assert.Equal(t, inserted.HypervisorID, fetched.HypervisorID)
				assert.Equal(t, inserted.FailureDomain, fetched.FailureDomain)
				assert.Equal(t, inserted.Name, fetched.Name)
				break
			}
		}
		assert.True(t, found, "Inserted device %s not found in GetAll response", inserted.ID)
	}

	log.Infof("Validated all %d inserted devices exist in GetAll response", len(mockDevices))
}

func TestPutAndGetPDU(t *testing.T) {
	c := newClient(t)

	pdus := []cpLib.PDU{
		{ID: "95f62aee-997e-11f0-9f1b-a70cff4b660b",
			Name:          "pdu-1",
			Location:      "us-west",
			PowerCapacity: "15Kw",
			Specification: "specification1",
		},
		{ID: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea",
			Name:          "pdu-2",
			Location:      "us-east",
			PowerCapacity: "15Kw",
			Specification: "specification2",
		},
	}

	for _, p := range pdus {
		resp, err := c.PutPDU(&p)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	res, err := c.GetPDUs(&cpLib.GetReq{GetAll: true})
	log.Info("resp from get pdus:", res)
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
}

func TestPutAndGetRack(t *testing.T) {
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

	resp, err := c.GetRacks(&cpLib.GetReq{GetAll: true})
	log.Info("GetRacks: ", resp)
	assert.NoError(t, err)
	assert.Equal(t, len(racks), len(resp), "Expected %d racks but got %d", len(racks), len(resp))

	// Validate each rack field-by-field
	for _, inserted := range racks {
		var found *cpLib.Rack
		for _, fetched := range resp {
			if fetched.ID == inserted.ID {
				found = &fetched
				break
			}
		}

		assert.NotNil(t, found, "Inserted rack with ID %s not found in GetRacks response", inserted.ID)

		// Detailed field comparisons
		assert.Equal(t, inserted.Name, found.Name, "Mismatch in Name for Rack ID %s", inserted.ID)
		assert.Equal(t, inserted.PDUID, found.PDUID, "Mismatch in PDUID for Rack ID %s", inserted.ID)
		assert.Equal(t, inserted.Location, found.Location, "Mismatch in Location for Rack ID %s", inserted.ID)
		assert.Equal(t, inserted.Specification, found.Specification, "Mismatch in Specification for Rack ID %s", inserted.ID)
	}

	log.Infof("All %d racks validated successfully", len(racks))
}

func TestPutAndGetHypervisor(t *testing.T) {
	c := newClient(t)

	hypervisors := []cpLib.Hypervisor{
		{RackID: "rack-1", ID: "89944570-ab2a-11f0-b55d-8fc2c05d35f4", IPAddress: "127.0.0.1", PortRange: "8000-9000", SSHPort: "6999", Name: "hv-1"},
		{RackID: "rack-2", ID: "8f70f2a4-ab2a-11f0-a1bb-cb25e1fa6a6b", IPAddress: "127.0.0.2", PortRange: "5000-7000", SSHPort: "7999", Name: "hv-2"},
	}

	for _, hv := range hypervisors {
		resp, err := c.PutHypervisor(&hv)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	resp, err := c.GetHypervisor(&cpLib.GetReq{GetAll: true})
	log.Info("GetHypervisor: ", resp)
	assert.NoError(t, err)

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
	vdev1 := &cpLib.Vdev{
		Size: 700 * 1024 * 1024 * 1024}
	err := c.CreateVdev(vdev1)
	log.Info("CreateMultiVdev Result 1: ", vdev1)
	VDEV_ID = vdev1.VdevID
	assert.NoError(t, err)

	vdev2 := &cpLib.Vdev{
		Size: 400 * 1024 * 1024 * 1024}
	err = c.CreateVdev(vdev2)
	log.Info("CreateMultiVdev Result 2: ", vdev2)
	assert.NoError(t, err)

	// Verify that IDs are unique
	assert.NotEqual(t, vdev1.VdevID, vdev2.VdevID, "Vdev IDs must be unique")
}

func TestGetAllVdev(t *testing.T) {
	c := newClient(t)
	req := &cpLib.GetReq{
		GetAll: true,
	}
	resp, err := c.GetVdevs(req)
	log.Info("fetch all vdevs response: ", resp)
	assert.NoError(t, err)
}

func TestGetSpecificVdev(t *testing.T) {
	c := newClient(t)
	req := &cpLib.GetReq{
		ID:     VDEV_ID,
		GetAll: false,
	}
	resp, err := c.GetVdevs(req)
	log.Info("vdevs response: ", resp)
	assert.NoError(t, err)

	v := resp[0]
	assert.Equal(t, VDEV_ID, v.VdevID, "Mismatch in Vdev ID")
}

func TestPutAndGetPartition(t *testing.T) {
	c := newClient(t)
	pt := &cpLib.DevicePartition{
		PartitionID:   "96ea4c60-a5df-11f0-a315-fb09c06e6471",
		DevID:         "nvme-Amazon_Elastic_Block_Store_vol0dce303259b3884dc",
		Size:          10 * 1024 * 1024 * 1024,
		PartitionPath: "some path",
		NISDUUID:      "b962cea8-ab42-11f0-a0ad-1bd216770b60",
	}
	resp, err := c.PutPartition(pt)
	log.Info("created partition: ", resp)
	assert.NoError(t, err)
	resp1, err := c.GetPartition(cpLib.GetReq{ID: "96ea4c60-a5df-11f0-a315-fb09c06e6471"})
	assert.NoError(t, err)
	log.Info("Get partition: ", resp1)

	// Validate the retrieved partition details
	assert.Equal(t, 1, len(resp1), "Expected exactly one partition in Get response")

	returned := resp1[0]

	assert.Equal(t, pt.PartitionID, returned.PartitionID, "Mismatch in PartitionID")
	assert.Equal(t, pt.PartitionPath, returned.PartitionPath, "Mismatch in PartitionPath")
	assert.Equal(t, pt.NISDUUID, returned.NISDUUID, "Mismatch in NISDUUID")
	assert.Equal(t, pt.DevID, returned.DevID, "Mismatch in DevID")
	assert.Equal(t, pt.Size, returned.Size, "Mismatch in Size (bytes)")
}

func runPutAndGetRack(b testing.TB, c *CliCFuncs) {
	racks := []cpLib.Rack{
		{ID: "rack-1", PDUID: "95f62aee-997e-11f0-9f1b-a70cff4b660b"},
		{ID: "rack-2", PDUID: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea"},
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

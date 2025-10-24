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
		resp, err := c.PutNisd(&n)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	res, err := c.GetNisds(cpLib.GetReq{ID: "nisd-002"})
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

	res, err := c.GetDevices(cpLib.GetReq{ID: "60447cd0-ab3e-11f0-aa15-1f40dd976538"})
	log.Infof("fetch single device info: %s, %s, %s", res[0].ID, res[0].HypervisorID, res[0].SerialNumber)
	assert.NoError(t, err)

	res, err = c.GetDevices(cpLib.GetReq{GetAll: true})
	log.Infof("fetech all device list: %s,%v", res[0].ID, res[0].Partitions)
	assert.NoError(t, err)

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
}

func TestVdevLifecycle(t *testing.T) {
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
		TotalSize:     15_000_000_000_000, // 1 TB
		AvailableSize: 15_000_000_000_000, // 750 GB
	}
	_, err := c.PutNisd(&n)
	assert.NoError(t, err)

	// Step 1: Create first Vdev
	vdev1 := &cpLib.Vdev{
		Size: 700 * 1024 * 1024 * 1024,
	}
	err = c.CreateVdev(vdev1)
	assert.NoError(t, err, "failed to create vdev1")
	assert.NotEmpty(t, vdev1.VdevID, "vdev1 ID should not be empty")
	log.Info("Created vdev1: ", vdev1)

	// Step 2: Create second Vdev
	vdev2 := &cpLib.Vdev{
		Size: 400 * 1024 * 1024 * 1024,
	}
	err = c.CreateVdev(vdev2)
	assert.NoError(t, err, "failed to create vdev2")
	assert.NotEmpty(t, vdev2.VdevID, "vdev2 ID should not be empty")
	log.Info("Created vdev2: ", vdev2)

	// Step 3: Fetch all Vdevs and validate both exist
	getAllReq := &cpLib.GetReq{GetAll: true}
	allResp, err := c.GetVdevs(getAllReq)
	assert.NoError(t, err, "failed to fetch all vdevs")
	assert.NotNil(t, allResp, "all vdevs response should not be nil")
	log.Info("All vdevs response: ", allResp)

	var found1, found2 bool
	for _, v := range allResp {
		if v.VdevID == vdev1.VdevID {
			found1 = true
			assert.Equal(t, vdev1.Size, v.Size, "vdev1 size mismatch")
		}
		if v.VdevID == vdev2.VdevID {
			found2 = true
			assert.Equal(t, vdev2.Size, v.Size, "vdev2 size mismatch")
		}
	}
	assert.True(t, found1, "vdev1 not found in GetAll response")
	assert.True(t, found2, "vdev2 not found in GetAll response")

	// Step 4: Fetch specific Vdev (vdev1)
	getSpecificReq := &cpLib.GetReq{
		ID:     vdev1.VdevID,
		GetAll: false,
	}
	specificResp, err := c.GetVdevs(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev")
	assert.NotNil(t, specificResp, "specific vdev response should not be nil")
	log.Info("Specific vdev response: ", specificResp)

	assert.Equal(t, 1, len(specificResp), "expected exactly one vdev in specific fetch")
	assert.Equal(t, vdev1.VdevID, specificResp[0].VdevID, "fetched vdev ID mismatch")
	assert.Equal(t, vdev1.Size, specificResp[0].Size, "fetched vdev size mismatch")
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

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

}

func TestPutAndGetDevice(t *testing.T) {
	c := newClient(t)

	mockDevices := []cpLib.Device{
		{
			ID:            "dev-001",
			SerialNumber:  "SN123456789",
			Status:        1,
			HypervisorID:  "hv-01",
			FailureDomain: "fd-01",
			NisdID:        "nisd-001",
		},
		{
			ID:            "dev-002",
			SerialNumber:  "SN987654321",
			Status:        0,
			HypervisorID:  "hv-02",
			FailureDomain: "fd-01",
			NisdID:        "nisd-002",
		},
		{
			ID:            "dev-003",
			SerialNumber:  "SN112233445",
			Status:        2,
			HypervisorID:  "hv-01",
			FailureDomain: "fd-02",
			NisdID:        "nisd-003",
		},
	}

	for _, p := range mockDevices {
		resp, err := c.PutDeviceInfo(&p)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	res, err := c.GetDeviceInfo(cpLib.GetReq{ID: "dev-002"})
	log.Infof("device info: %s, %s, %s", res[0].ID, res[0].HypervisorID, res[0].SerialNumber)
	assert.NoError(t, err)

}

func TestPutAndGetPDU(t *testing.T) {
	c := newClient(t)

	pdus := []cpLib.PDU{
		{ID: "95f62aee-997e-11f0-9f1b-a70cff4b660b"},
		{ID: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea"},
	}

	for _, p := range pdus {
		resp, err := c.PutPDU(&p)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	res, err := c.GetPDUs(&cpLib.GetReq{GetAll: true})
	log.Info("GetPDUs: ", res)
	assert.NoError(t, err)
}

func TestPutAndGetRack(t *testing.T) {
	c := newClient(t)

	racks := []cpLib.Rack{
		{ID: "rack-1", PDUID: "95f62aee-997e-11f0-9f1b-a70cff4b660b"},
		{ID: "rack-2", PDUID: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea"},
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
		{RackID: "rack-1", ID: "hv-1", IPAddress: "127.0.0.1"},
		{RackID: "rack-2", ID: "hv-2", IPAddress: "127.0.0.2"},
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

func TestMultiCreateVdev(t *testing.T) {
	c := newClient(t)
	vdev1 := &cpLib.Vdev{
		Size: 700 * 1024 * 1024 * 1024}
	err := c.CreateVdev(vdev1)
	log.Info("CreateMultiVdev Result 1: ", vdev1)
	assert.NoError(t, err)

	vdev2 := &cpLib.Vdev{
		Size: 400 * 1024 * 1024 * 1024}
	err = c.CreateVdev(vdev2)
	log.Info("CreateMultiVdev Result 2: ", vdev2)
	assert.NoError(t, err)
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

// func TestGetSpecificVdev(t *testing.T) {
// 	c := newClient(t)
// 	req := &cpLib.GetReq{
// 		ID:     VDEV_ID,
// 		GetAll: false,
// 	}
// 	resp, err := c.GetVdevs(req)
// 	log.Info("vdevs response: ", resp)
// 	assert.NoError(t, err)
// }

func TestPutPartition(t *testing.T) {
	c := newClient(t)
	pt := &cpLib.DevicePartition{
		PartitionUUID: "96ea4c60-a5df-11f0-a315-fb09c06e6471",
		DevID:         "nvme-Amazon_Elastic_Block_Store_vol0dce303259b3884dc",
		Size:          10 * 1024 * 1024 * 1024,
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

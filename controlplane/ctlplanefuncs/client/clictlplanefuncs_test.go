package clictlplanefuncs

import (
	"fmt"
	"os"
	"path"
	"testing"
	"context"
    "time"
    "golang.org/x/sync/errgroup"
	"fmt"
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

func TestPutAndGetNisd(t *testing.T) {
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
			HypervisorID:  "hv-1",
			FailureDomain: "fd-01",
			DevicePath:    "/temp/path1",
			Name:          "dev-1",
		},
		{
			ID:            "6bd604a6-ab3e-11f0-805a-3f086c1f2d21",
			SerialNumber:  "SN987654321",
			State:         0,
			HypervisorID:  "hv-2",
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
			HypervisorID:  "hv-1",
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

	// PUT multiple devices
	for _, d := range mockDevices {
		resp, err := c.PutDevice(&d)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	// GET all devices
	res, err := c.GetDevices(cpLib.GetReq{})
	log.Infof("fetch single device info: %s, %s, %s", res[0].ID, res[0].HypervisorID, res[0].SerialNumber)
=======
	// GET all devices
	res, err := c.GetDeviceInfo(cpLib.GetReq{})
>>>>>>> 955463c ( Added two sub-tests for each entity.)
	assert.NoError(t, err)
	assert.NotEmpty(t, res)

	// Store results in the map
	for _, d := range res {
		Devices[d.ID] = d
	}

	// Validation: Device ↔ Hypervisor
	for _, device := range Devices {
		var hvExists bool
		for _, hv := range Hypervisors {
			if hv.Name == device.HypervisorID {
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
	_, err := c.PutNisd(&n)
	assert.NoError(t, err)

	// Step 1: Create first Vdev
	vdev1 := &cpLib.Vdev{
		Cfg: cpLib.VdevCfg{
			Size: 700 * 1024 * 1024 * 1024,
		}}
	err = c.CreateVdev(vdev1)
	assert.NoError(t, err, "failed to create vdev1")
	assert.NotEmpty(t, vdev1.Cfg.ID, "vdev1 ID should not be empty")
	log.Info("Created vdev1: ", vdev1)

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
	specificResp, err := c.GetVdevCfg(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev")
	assert.NotNil(t, specificResp, "specific vdev response should not be nil")
	log.Info("Specific vdev response: ", specificResp)

	assert.Equal(t, vdev1.Cfg.ID, specificResp.ID, "fetched vdev ID mismatch")
	assert.Equal(t, vdev1.Cfg.Size, specificResp.Size, "fetched vdev size mismatch")

	result, err := c.GetVdevsWithChunkInfo(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev with chunk mapping")
	assert.NotNil(t, result, "specific vdev with chunk mapping response should not be nil")
	log.Info("Specific vdev with chunk mapping response: ", result)

	assert.Equal(t, 1, len(result), "expected exactly one vdev with chunk mapping in specific fetch")
	assert.Equal(t, vdev1.Cfg.ID, result[0].Cfg.ID, "fetched vdev ID mismatch")
	assert.Equal(t, vdev1.Cfg.Size, result[0].Cfg.Size, "fetched vdev size mismatch")
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

func TestVdevNisdChunk(t *testing.T) {

	c := newClient(t)

	// // create nisd
	// mockNisd := cpLib.Nisd{
	// 	ClientPort: 7001,
	// 	PeerPort:   8001,
	// 	ID:         "nisd-001",
	// 	FailureDomain: []string{
	// 		"pdu-05",
	// 		"rack-04",
	// 		"hv-07",
	// 		"dev-004",
	// 	},
	// 	IPAddr:        "192.168.1.10",
	// 	TotalSize:     1_000_000_000_000, // 1 TB
	// 	AvailableSize: 750_000_000_000,   // 750 GB
	// }
	// resp, err := c.PutNisd(&mockNisd)
	// assert.NoError(t, err)
	// assert.True(t, resp.Success)

	// // create vdev
	// vdev := &cpLib.Vdev{
	// 	Cfg: cpLib.VdevCfg{
	// 		Size: 500 * 1024 * 1024 * 1024,
	// 	}}
	// err = c.CreateVdev(vdev)
	// log.Info("Created Vdev Result: ", vdev)
	// assert.NoError(t, err)
	// readV, err := c.GetVdevCfg(&cpLib.GetReq{ID: vdev.Cfg.ID})
	// log.Info("Read vdev:", readV)
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

func TestHierarchy2(t *testing.T) {
	c := newClient(t)

	pdus := []string{
		"11111111-d458-11f0-ae93-fb6358160001",
		"11111111-d458-11f0-ae93-fb6358160002",
		"11111111-d458-11f0-ae93-fb6358160003",
		"11111111-d458-11f0-ae93-fb6358160004",
		"11111111-d458-11f0-ae93-fb6358160005",
	}

	// 10 RACKS
	racks := []string{
		"22222222-d458-11f0-ae93-fb6358161001",
		"22222222-d458-11f0-ae93-fb6358161002",
		"22222222-d458-11f0-ae93-fb6358161003",
		"22222222-d458-11f0-ae93-fb6358161004",
		"22222222-d458-11f0-ae93-fb6358161005",
		"22222222-d458-11f0-ae93-fb6358161006",
		"22222222-d458-11f0-ae93-fb6358161007",
		"22222222-d458-11f0-ae93-fb6358161008",
		"22222222-d458-11f0-ae93-fb6358161009",
		"22222222-d458-11f0-ae93-fb6358161010",
	}

	// 20 HVs
	hvs := []string{
		"33333333-d458-11f0-ae93-fb6358162001",
		"33333333-d458-11f0-ae93-fb6358162002",
		"33333333-d458-11f0-ae93-fb6358162003",
		"33333333-d458-11f0-ae93-fb6358162004",
		"33333333-d458-11f0-ae93-fb6358162005",
		"33333333-d458-11f0-ae93-fb6358162006",
		"33333333-d458-11f0-ae93-fb6358162007",
		"33333333-d458-11f0-ae93-fb6358162008",
		"33333333-d458-11f0-ae93-fb6358162009",
		"33333333-d458-11f0-ae93-fb6358162010",
		"33333333-d458-11f0-ae93-fb6358162011",
		"33333333-d458-11f0-ae93-fb6358162012",
		"33333333-d458-11f0-ae93-fb6358162013",
		"33333333-d458-11f0-ae93-fb6358162014",
		"33333333-d458-11f0-ae93-fb6358162015",
		"33333333-d458-11f0-ae93-fb6358162016",
		"33333333-d458-11f0-ae93-fb6358162017",
		"33333333-d458-11f0-ae93-fb6358162018",
		"33333333-d458-11f0-ae93-fb6358162019",
		"33333333-d458-11f0-ae93-fb6358162020",
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
							ClientPort: 7000 + uint16(nisdID),
							PeerPort:   8000 + uint16(nisdID),
							ID:         fmt.Sprintf("ed7914c3-2e96-4f3e-8e0d-%012x", nisdID),
							FailureDomain: []string{
								pdu,
								rack,
								hv,
								dev,
							},
							IPAddr:        fmt.Sprintf("192.168.1.%d", ((nisdID-1)%250)+1),
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

	for _, n := range mockNisd {
		resp, err := c.PutNisd(&n)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

}

func TestHierarchyNew(t *testing.T) {
	c := newClient(t)

	pdus := []string{
		"11111111-d458-11f0-ae93-fb6358160001",
		"11111111-d458-11f0-ae93-fb6358160002",
	}

	// 10 RACKS
	racks := []string{
		"22222222-d458-11f0-ae93-fb6358161001",
		"22222222-d458-11f0-ae93-fb6358161002",
	}

	// 20 HVs
	hvs := []string{
		"33333333-d458-11f0-ae93-fb6358162001",
		"33333333-d458-11f0-ae93-fb6358162002",
		"33333333-d458-11f0-ae93-fb6358162003",
		"33333333-d458-11f0-ae93-fb6358162004",
		"33333333-d458-11f0-ae93-fb6358162005",
	}

	// 40 Devices
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
			ClientPort: 7000,
			PeerPort:   8000,
			ID:         "86adee3a-d5da-11f0-8250-5f1ad86a5661",
			FailureDomain: []string{
				pdus[0],
				racks[0],
				hvs[0],
				devices[0],
			},
			IPAddr:        "192.168.1.1",
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		cpLib.Nisd{
			ClientPort: 7000,
			PeerPort:   8000,
			ID:         "86adee3a-d5da-11f0-8250-5f1ad86a5662",
			FailureDomain: []string{
				pdus[0],
				racks[0],
				hvs[0],
				devices[1],
			},
			IPAddr:        "192.168.1.1",
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		cpLib.Nisd{
			ClientPort: 7000,
			PeerPort:   8000,
			ID:         "86adee3a-d5da-11f0-8250-5f1ad86a5663",
			FailureDomain: []string{
				pdus[0],
				racks[0],
				hvs[1],
				devices[2],
			},
			IPAddr:        "192.168.1.1",
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		cpLib.Nisd{
			ClientPort: 7000,
			PeerPort:   8000,
			ID:         "86adee3a-d5da-11f0-8250-5f1ad86a5664",
			FailureDomain: []string{
				pdus[1],
				racks[1],
				hvs[2],
				devices[3],
			},
			IPAddr:        "192.168.1.1",
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		cpLib.Nisd{
			ClientPort: 7000,
			PeerPort:   8000,
			ID:         "86adee3a-d5da-11f0-8250-5f1ad86a5665",
			FailureDomain: []string{
				pdus[1],
				racks[1],
				hvs[3],
				devices[4],
			},
			IPAddr:        "192.168.1.1",
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		cpLib.Nisd{
			ClientPort: 7000,
			PeerPort:   8000,
			ID:         "86adee3a-d5da-11f0-8250-5f1ad86a5666",
			FailureDomain: []string{
				pdus[1],
				racks[1],
				hvs[4],
				devices[5],
			},
			IPAddr:        "192.168.1.1",
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
	}
	for _, n := range mockNisd {
		resp, err := c.PutNisd(&n)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

}

func TestCreateVdev(t *testing.T) {
	c := newClient(t)
	vdev := &cpLib.Vdev{
		Cfg: cpLib.VdevCfg{
			Size:       268435456000,
			NumReplica: 2,
		},
	}

	err := c.CreateVdev(vdev)
	assert.NoError(t, err)
	log.Info("successfully created vdev: ", vdev)
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

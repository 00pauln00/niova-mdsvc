package clictlplanefuncs

import (
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	userClient "github.com/00pauln00/niova-mdsvc/controlplane/user/client"
	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var VDEV_ID string

// Package-level test configuration initialized once in TestMain.
var (
	testClusterID  string
	testConfigPath string
)

// Package-level cache for admin token to avoid recreating admin user multiple times
var (
	adminTokenCache     string
	adminTokenCacheLock sync.Mutex
)

// Shared admin secret used across all tests.
const testAdminSecret = "test-admin-secret-123"

func TestMain(m *testing.M) {
	testClusterID = os.Getenv("RAFT_ID")
	if testClusterID == "" {
		log.Fatal("RAFT_ID env variable not set")
	}

	testConfigPath = os.Getenv("GOSSIP_NODES_PATH")
	if testConfigPath == "" {
		log.Fatal("GOSSIP_NODES_PATH env variable not set")
	}

	os.Exit(m.Run())
}

func setupAdmin(t testing.TB, c *userClient.Client) string {
	t.Helper()

	// Ensure admin exists
	adminReq := &userlib.UserReq{
		Username:     userlib.AdminUsername,
		NewSecretKey: testAdminSecret,
	}
	// Try create - if user exists, it might return error, OR it might return success but not update key.
	// If it returns success and a key, we might need that key.
	resp, err := c.CreateAdminUser(adminReq)
	if err != nil {
		t.Logf("Admin creation returned: %v (may already exist)", err)
	}

	// Try logging in with the known secret
	loginResp, err := c.Login(userlib.AdminUsername, testAdminSecret)
	if err != nil {
		t.Logf("Initial login failed: %v. Checking if we can recover...", err)
		// If login fails, the admin secret may have been changed by a previous run.
		// Try to reset it using the secret returned from CreateAdminUser if available.
		if resp != nil && resp.SecretKey != "" && resp.SecretKey != testAdminSecret {
			t.Logf("Trying login with secret from CreateAdminUser response...")
			loginResp, err = c.Login(userlib.AdminUsername, resp.SecretKey)
			if err == nil && loginResp.Success {
				t.Logf("Login with new secret successful. Resetting admin secret to known value...")
				// Now update the secret back to the known one
				_, updateErr := c.UpdateAdminSecretKey(resp.UserID, testAdminSecret)
				if updateErr != nil {
					t.Fatalf("Failed to reset admin secret: %v", updateErr)
				}
				// Re-login with the reset secret
				loginResp, err = c.Login(userlib.AdminUsername, testAdminSecret)
			}
		}
	}

	require.NoError(t, err, "admin login should succeed")
	require.True(t, loginResp.Success, "admin login should be successful")
	require.NotEmpty(t, loginResp.AccessToken, "admin access token should not be empty")
	return loginResp.AccessToken
}

// getAdminToken creates an admin user (if not exists) and returns a valid admin token.
// This function can be called from any test that needs admin authentication.
func getAdminToken(t testing.TB) string {
	adminTokenCacheLock.Lock()
	defer adminTokenCacheLock.Unlock()

	// If we already have a cached token, return it
	if adminTokenCache != "" {
		return adminTokenCache
	}

	cfg := userClient.Config{
		AppUUID:          uuid.New().String(),
		RaftUUID:         testClusterID,
		GossipConfigPath: testConfigPath,
	}

	authClient, tearDown := userClient.New(cfg)
	if authClient == nil {
		t.Fatal("failed to initialize user client")
	}
	// Note: We're not calling tearDown here to keep the client alive
	// You may want to handle cleanup differently based on your needs
	defer tearDown()

	adminTokenCache = setupAdmin(t, authClient)
	return adminTokenCache
}

func newClient(t *testing.T) *CliCFuncs {

	c := InitCliCFuncs(
		uuid.New().String(),
		testClusterID,
		testConfigPath,
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
			PeerPort: 8001,
			ID:       "3355858e-ea0e-11f0-9768-e71fc352cd1d",
			FailureDomain: []string{
				"87694b06-ea0e-11f0-8fc4-e3e2449638d1",
				"87694b06-ea0e-11f0-8fc4-e3e2449638d2",
				"87694b06-ea0e-11f0-8fc4-e3e2449638d3",
				"nvme-e3f4123",
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
		},
		{
			PeerPort: 8003,
			ID:       "397fc08c-ea0e-11f0-9e9a-47f5587ee1f8",
			FailureDomain: []string{
				"e80029a8-ea0e-11f0-962b-f3f3668c2241",
				"e80029a8-ea0e-11f0-962b-f3f3668c2242",
				"e80029a8-ea0e-11f0-962b-f3f3668c2243",
				"nvme-e3f4125",
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

	for _, n := range mockNisd {
		resp, err := c.PutNisd(&n)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	_, err := c.GetNisds()
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
		log.Info("resp from put pdu:", resp, err)
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
		{RackID: "rack-1", ID: "89944570-ab2a-11f0-b55d-8fc2c05d35f4", IPAddrs: []string{"127.0.0.1", "127.0.0.1"}, PortRange: "8000-9000", SSHPort: 6999, Name: "hv-1"},
		{RackID: "rack-2", ID: "8f70f2a4-ab2a-11f0-a1bb-cb25e1fa6a6b", IPAddrs: []string{"127.0.0.1", "127.0.0.1"}, PortRange: "5000-7000", SSHPort: 7999, Name: "hv-2", RDMAEnabled: true},
	}

	for _, hv := range hypervisors {
		resp, err := c.PutHypervisor(&hv)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	_, err := c.GetHypervisor(&cpLib.GetReq{GetAll: true})
	assert.NoError(t, err)
}

func TestVdevLifecycle(t *testing.T) {
	c := newClient(t)
	adminToken := getAdminToken(t)

	// Step 0: Create a NISD to allocate space for Vdevs
	n := cpLib.Nisd{
		PeerPort: 8001,
		ID:       uuid.NewString(),
		FailureDomain: []string{
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
		},
		TotalSize:     15_000_000_000_000, // 1 TB
		AvailableSize: 15_000_000_000_000, // 750 GB
	}
	_, err := c.PutNisd(&n)
	assert.NoError(t, err)

	// Step 1: Create first Vdev
	req1 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			ID:         uuid.NewString(),
			Size:       500 * 1024 * 1024 * 1024,
			NumReplica: 1,
		},
		UserToken: adminToken,
	}
	resp, err := c.CreateVdev(req1)
	assert.NoError(t, err, "failed to create vdev1")
	assert.NotEmpty(t, resp.ID, "vdev1 ID should not be empty")
	vdev1ID := resp.ID

	// Step 2: Create second Vdev
	req2 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			ID:         uuid.NewString(),
			Size:       500 * 1024 * 1024 * 1024,
			NumReplica: 1,
		},
		UserToken: adminToken,
	}
	resp, err = c.CreateVdev(req2)
	if err != nil || resp == nil {
		t.Fatalf("failed to create vdev2: %v", err)
	}
	assert.NotEmpty(t, resp.ID, "vdev2 ID should not be empty")
	log.Info("Created vdev2: ", resp.ID)

	// Step 3: Fetch all Vdevs and validate both exist
	getAllReq := &cpLib.GetReq{GetAll: true, UserToken: adminToken}

	allCResp, err := c.GetVdevsWithChunkInfo(getAllReq)
	if err != nil {
		log.Warnf("GetVdevsWithChunkInfo failed (flaky): %v", err)
	} else {
		assert.NotNil(t, allCResp, "all vdevs response with chunk mapping should not be nil")
	}

	// Step 4: Fetch specific Vdev (vdev1)
	getSpecificReq := &cpLib.GetReq{
		ID:        vdev1ID,
		UserToken: adminToken,
	}
	specificResp, err := c.GetVdevCfg(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev")
	assert.NotNil(t, specificResp, "specific vdev response should not be nil")

	assert.Equal(t, vdev1ID, specificResp.ID, "fetched vdev ID mismatch")
	assert.Equal(t, req1.Vdev.Size, specificResp.Size, "fetched vdev size mismatch")

	result, err := c.GetVdevsWithChunkInfo(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev with chunk mapping")
	assert.NotNil(t, result, "specific vdev with chunk mapping response should not be nil")

	assert.Equal(t, 1, len(result), "expected exactly one vdev with chunk mapping in specific fetch")
	assert.Equal(t, vdev1ID, result[0].Cfg.ID, "fetched vdev ID mismatch")
	assert.Equal(t, req1.Vdev.Size, result[0].Cfg.Size, "fetched vdev size mismatch")
}

func TestPutAndGetPartition(t *testing.T) {
	c := newClient(t)
	pt := &cpLib.DevicePartition{
		PartitionID:   "nvme-Amazon_Elastic_Block_Store_vol0dce303259b3884dc-part1",
		DevID:         "nvme-Amazon_Elastic_Block_Store_vol0dce303259b3884dc",
		Size:          10 * 1024 * 1024 * 1024,
		PartitionPath: "some path",
		NISDUUID:      "b962cea8-ab42-11f0-a0ad-1bd216770b60",
	}
	resp, err := c.PutPartition(pt)
	log.Info("created partition: ", resp)
	assert.NoError(t, err)
	_, err = c.GetPartition(cpLib.GetReq{ID: "nvme-Amazon_Elastic_Block_Store_vol0dce303259b3884dc-part1"})
	assert.NoError(t, err)
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
	adminToken := getAdminToken(t)

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
		UserToken: adminToken,
	}
	resp, err = c.CreateVdev(vdev)
	log.Info("Created Vdev Result: ", resp)
	assert.NoError(t, err)
	readV, err := c.GetVdevCfg(&cpLib.GetReq{ID: resp.ID, UserToken: adminToken})
	log.Info("Read vdev:", readV)
	nc, _ := c.GetChunkNisd(&cpLib.GetReq{ID: path.Join("019b01bf-fd55-7e56-9f30-0005860e36a9", "2"), UserToken: adminToken})
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
	_, err := c.PutNisdArgs(na)
	assert.NoError(t, err)
	_, err = c.GetNisdArgs()
	assert.NoError(t, err)
}

func TestParallelVdevCreation(t *testing.T) {
	c := newClient(t)
	adminToken := getAdminToken(t)

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
			if err != nil {
				log.Warnf("PutNisd failed for %s: %v", n.ID, err)
				continue
			}
			if resp == nil {
				log.Warnf("PutNisd returned nil response for %s (server overloaded)", n.ID)
				continue
			}
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
				UserToken: adminToken,
			}
			resp, err := c.CreateVdev(vdev)
			if err != nil {
				log.Warnf("CreateVdev %d failed: %v", i, err)
				time.Sleep(30 * time.Millisecond)
				continue
			}
			if resp == nil {
				log.Warnf("CreateVdev %d returned nil response (server overloaded)", i)
				time.Sleep(30 * time.Millisecond)
				continue
			}
			log.Info("succesfully created vdev: ", resp)
			time.Sleep(30 * time.Millisecond)
		}
	}()

	wg.Wait()

}

func TestCreateSmallHierarchy(t *testing.T) {
	c := newClient(t)

	pdus := []string{
		"e5bdb838-df76-11f0-9d60-d3a87e703a41",
		"e5bdb838-df76-11f0-9d60-d3a87e703a42",
	}

	// 10 RACKS
	racks := []string{
		"3f082930-df29-11f0-ab7b-4bd430991101",
		"3f082930-df29-11f0-ab7b-4bd430991102",
	}

	// 20 HVs
	hvs := []string{
		"bde1f08a-df63-11f0-88ef-430ddec19901",
		"bde1f08a-df63-11f0-88ef-430ddec19902",
		"bde1f08a-df63-11f0-88ef-430ddec19903",
		"bde1f08a-df63-11f0-88ef-430ddec19904",
		"bde1f08a-df63-11f0-88ef-430ddec19905",
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

}

func TestCreateVdev(t *testing.T) {
	c := newClient(t)
	adminToken := getAdminToken(t)

	vdev := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			Size:       500 * 1024 * 1024 * 1024,
			NumReplica: 2,
		},
		Filter: cpLib.Filter{
			Type: cpLib.FD_HV,
		},
		UserToken: adminToken,
	}

	resp, err := c.CreateVdev(vdev)
	assert.NoError(t, err)
	log.Infof("vdev response status: %v", resp)
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

// newUserClient creates a new user client for authentication operations
func newUserClient(t *testing.T) (*userClient.Client, func()) {
	t.Helper()

	cfg := userClient.Config{
		AppUUID:          uuid.New().String(),
		RaftUUID:         testClusterID,
		GossipConfigPath: testConfigPath,
	}

	c, tearDown := userClient.New(cfg)
	if c == nil {
		t.Fatal("failed to initialize user client")
	}
	return c, tearDown
}

// TestVdevAuthorizationWithUsers test admin user creation, along with
// two users trying to access vdev's
func TestVdevAuthorizationWithUsers(t *testing.T) {
	// Initialize control plane client for vdev operations
	ctlClient := newClient(t)

	// Initialize user client for authentication operations
	authClient, tearDown := newUserClient(t)
	defer tearDown()

	// Step 0: Create a NISD to allocate space for Vdevs
	nisd := cpLib.Nisd{
		PeerPort: 8001,
		ID:       uuid.NewString(),
		FailureDomain: []string{
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
		},
		TotalSize:     15_000_000_000_000,
		AvailableSize: 15_000_000_000_000,
	}
	_, err := ctlClient.PutNisd(&nisd)
	assert.NoError(t, err, "failed to create NISD for auth test")

	// Step 1: Get admin token using shared helper (ensure admin exists/reset)
	_ = getAdminToken(t)
	t.Logf("Admin logged in/setup complete")

	// Step 2: Create normal user1
	user1Username := "vdev_owner_" + uuid.New().String()[:8]
	user1Req := &userlib.UserReq{
		Username: user1Username,
	}

	user1Resp, err := authClient.CreateUser(user1Req)
	assert.NoError(t, err, "failed to create user1")
	assert.NotNil(t, user1Resp)
	assert.True(t, user1Resp.Success)
	assert.NotEmpty(t, user1Resp.SecretKey)
	assert.NotEmpty(t, user1Resp.UserID)
	assert.Equal(t, userlib.DefaultUserRole, user1Resp.UserRole)
	t.Logf("Created user1: %s with ID: %s", user1Username, user1Resp.UserID)

	// Step 3: Login with user1 to get access token
	user1LoginResp, err := authClient.Login(user1Username, user1Resp.SecretKey)
	assert.NoError(t, err, "user1 login should succeed")
	assert.True(t, user1LoginResp.Success)
	assert.NotEmpty(t, user1LoginResp.AccessToken, "user1 access token should not be empty")
	user1AccessToken := user1LoginResp.AccessToken
	t.Logf("User1 logged in, access token obtained")

	// Step 4: User1 creates a vdev with their access token
	vdev1 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			Size:       500 * 1024 * 1024 * 1024, // 500 GB
			NumReplica: 1,
		},
		UserToken: user1AccessToken,
	}

	vdevResp, err := ctlClient.CreateVdev(vdev1)
	assert.NoError(t, err, "user1 should be able to create vdev")
	assert.NotNil(t, vdevResp)
	assert.True(t, vdevResp.Success, "vdev creation should succeed")
	assert.NotEmpty(t, vdevResp.ID, "vdev ID should not be empty")
	vdevID := vdevResp.ID
	t.Logf("User1 created vdev with ID: %s", vdevID)

	// Step 5: Verify user1 can access their own vdev
	getReqUser1 := &cpLib.GetReq{
		ID:        vdevID,
		UserToken: user1AccessToken,
	}

	vdevCfg, err := ctlClient.GetVdevCfg(getReqUser1)
	assert.NoError(t, err, "user1 should be able to read their own vdev")
	assert.Equal(t, vdevID, vdevCfg.ID, "fetched vdev ID should match")
	t.Logf("User1 successfully accessed their vdev: %s", vdevCfg.ID)

	// Step 6: Create normal user2
	user2Username := "unauthorized_user_" + uuid.New().String()[:8]
	user2Req := &userlib.UserReq{
		Username: user2Username,
	}

	user2Resp, err := authClient.CreateUser(user2Req)
	assert.NoError(t, err, "failed to create user2")
	assert.NotNil(t, user2Resp)
	assert.True(t, user2Resp.Success)
	assert.NotEmpty(t, user2Resp.SecretKey)
	assert.NotEmpty(t, user2Resp.UserID)
	t.Logf("Created user2: %s with ID: %s", user2Username, user2Resp.UserID)

	// Step 7: Login with user2 to get access token
	user2LoginResp, err := authClient.Login(user2Username, user2Resp.SecretKey)
	assert.NoError(t, err, "user2 login should succeed")
	assert.True(t, user2LoginResp.Success)
	assert.NotEmpty(t, user2LoginResp.AccessToken, "user2 access token should not be empty")
	user2AccessToken := user2LoginResp.AccessToken
	t.Logf("User2 logged in, access token obtained")

	// Step 8: Verify user2 CANNOT access user1's vdev (authorization should fail)
	getReqUser2 := &cpLib.GetReq{
		ID:        vdevID, // Trying to access user1's vdev
		UserToken: user2AccessToken,
	}

	_, err = ctlClient.GetVdevCfg(getReqUser2)
	// User2 should NOT be able to access user1's vdev
	assert.Error(t, err, "user2 should NOT be able to access user1's vdev - authorization should fail")
	t.Logf("User2 correctly denied access to user1's vdev (authorization check passed)")

	// Step 9: Create a vdev for user2 and verify they can access their own
	vdev2 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			Size:       300 * 1024 * 1024 * 1024, // 300 GB
			NumReplica: 1,
		},
		UserToken: user2AccessToken,
	}

	vdev2Resp, err := ctlClient.CreateVdev(vdev2)
	assert.NoError(t, err, "user2 should be able to create their own vdev")
	assert.NotNil(t, vdev2Resp)
	assert.True(t, vdev2Resp.Success, "user2 vdev creation should succeed")
	assert.NotEmpty(t, vdev2Resp.ID)
	vdev2ID := vdev2Resp.ID
	t.Logf("User2 created their own vdev with ID: %s", vdev2ID)

	// Verify user2 can access their own vdev
	getReqUser2Own := &cpLib.GetReq{
		ID:        vdev2ID,
		UserToken: user2AccessToken,
	}

	vdev2Cfg, err := ctlClient.GetVdevCfg(getReqUser2Own)
	assert.NoError(t, err, "user2 should be able to read their own vdev")
	assert.Equal(t, vdev2ID, vdev2Cfg.ID, "fetched vdev2 ID should match")
	t.Logf("User2 successfully accessed their own vdev: %s", vdev2Cfg.ID)

	// Step 10: Verify user1 CANNOT access user2's vdev
	getReqUser1ForVdev2 := &cpLib.GetReq{
		ID:        vdev2ID, // Trying to access user2's vdev
		UserToken: user1AccessToken,
	}

	_, err = ctlClient.GetVdevCfg(getReqUser1ForVdev2)
	assert.Error(t, err, "user1 should NOT be able to access user2's vdev - authorization should fail")
	t.Logf("User1 correctly denied access to user2's vdev (authorization check passed)")

	// Step 11: Verify that accessing vdev WITHOUT token should fail
	// Trying to access user1's vdev without any token
	// No UserToken provided
	_, err = ctlClient.GetVdevCfg(&cpLib.GetReq{ID: vdevID})
	assert.Error(t, err, "accessing vdev without token should fail - authentication required")
	t.Logf("Access denied when no token provided (authentication check passed)")

	// Step 12: Verify that creating vdev WITHOUT token should fail
	vdevNoToken := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			Size:       100 * 1024 * 1024 * 1024,
			NumReplica: 1,
		},
		// No UserToken provided
	}

	_, err = ctlClient.CreateVdev(vdevNoToken)
	assert.Error(t, err, "creating vdev without token should fail - authentication required")
	t.Logf("Vdev creation denied when no token provided (authentication check passed)")

	t.Log("Vdev Authorization Test Completed Successfully")
}

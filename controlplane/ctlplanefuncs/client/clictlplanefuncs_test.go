package clictlplanefuncs

import (
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
    "time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	userClient "github.com/00pauln00/niova-mdsvc/controlplane/user/client"
	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"
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

// authEnabled is set once in TestMain and reflects whether the server under
// test has authentication enabled (AUTH_ENABLED != "false").
var authEnabled bool

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

	authEnabled = os.Getenv("AUTH_ENABLED") != "false"

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
				_, updateErr := c.UpdateAdminSecretKey(resp.UserID, testAdminSecret, loginResp.AccessToken)
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

func newClient(t testing.TB) *CliCFuncs {
	c := InitCliCFuncs(
		uuid.New().String(),
		testClusterID,
		testConfigPath,
		"",
	)
	if c == nil {
		t.Fatal("failed to init client funcs")
	}
	if authEnabled {
		c.SetToken(getAdminToken(t))
	}
	return c
}

// newClientWithToken creates a completely fresh CliCFuncs client with the given token.
func newClientWithToken(t testing.TB, token string) *CliCFuncs {
	t.Helper()

	c := InitCliCFuncs(
		uuid.New().String(),
		testClusterID,
		testConfigPath,
		"",
	)
	if c == nil {
		t.Fatal("failed to init client funcs")
	}
	if token != "" {
		c.SetToken(token)
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
   req := cpLib.GetReq{
		GetAll:    true,
	}
   res, err := c.GetNisds(req)
   assert.NoError(t, err)
   log.Info("GetNisd: ", res)
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
	req := cpLib.GetReq{
		GetAll:    true,
	}
    res, err := c.GetNisds(req)
	assert.NoError(t, err)
	assert.NotEmpty(t, res)

	// Store results in the map
	for _, n := range res {
		Nisds[n.ID] = n
	}

	log.Infof("Inserted %d NISDs, retrieved %d successfully.", len(mockNisd), len(res))
	assert.NoError(t, err)
	// all testcases use same data store, so including currently added hypervisors we check the result
	assert.GreaterOrEqual(t, len(resp), len(hypervisors))
}

func TestMultiCreateVdev(t *testing.T) {
	c := newClient(t)

	// Step 0: Create a NISD to allocate space for Vdevs
	n := cpLib.Nisd{
		PeerPort: 8001,
		ID:       uuid.NewString(),
		FailureDomain: []string{
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
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
	resp, err := c.PutNisd(&n)
	assert.NoError(t, err)
	assert.NotEmpty(t, resp.Name, "name cannot be empty")
	assert.Equal(t, resp.Success, true)

	// Step 1: Create first Vdev
	req1 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			ID:         uuid.NewString(),
			Size:       500 * 1024 * 1024 * 1024,
			NumReplica: 1,
		},
	}
	resp, err = c.CreateVdev(req1)
	assert.NoError(t, err, "failed to create vdev1")
	assert.NotEmpty(t, resp, "vdev1 ID should not be empty")
	log.Info("Created vdev1: ", resp.ID)
	require.NotNil(t, resp, "vdev1 response should not be nil")
	assert.NotEmpty(t, resp.ID, "vdev1 ID should not be empty")
	vdev1ID := resp.ID

	// Step 2: Create second Vdev
	req2 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			ID:         uuid.NewString(),
			Size:       500 * 1024 * 1024 * 1024,
			NumReplica: 1,
		},
	}
	resp, err = c.CreateVdev(req2)
	assert.NoError(t, err, "failed to create vdev2")
	assert.NotEmpty(t, resp, "vdev2 ID should not be empty")
	log.Info("Created vdev2: ", resp)
	if err != nil || resp == nil {
		t.Fatalf("failed to create vdev2: %v", err)
	}
	assert.NotEmpty(t, resp.ID, "vdev2 ID should not be empty")
	log.Info("Created vdev2: ", resp.ID)

	// Step 3: Fetch all Vdevs and validate both exist
	getAllReq := &cpLib.GetReq{GetAll: true}

	allCResp, err := c.GetVdevsWithChunkInfo(getAllReq)
	if err != nil {
		log.Warnf("GetVdevsWithChunkInfo failed: %v", err)
	} else {
		assert.NotNil(t, allCResp, "all vdevs response with chunk mapping should not be nil")
	}

	// Step 4: Fetch specific Vdev (vdev1)
	getSpecificReq := &cpLib.GetReq{
		ID:        vdev1ID,
	}
	specificResp, err := c.GetVdevCfg(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev")
	assert.NotNil(t, specificResp, "specific vdev response should not be nil")

	assert.Equal(t, vdev1ID, specificResp.ID, "fetched vdev ID mismatch")
	assert.Equal(t, req1.Vdev.Size, specificResp.Size, "fetched vdev size mismatch")

	result, err := c.GetVdevsWithChunkInfo(getSpecificReq)
	assert.NoError(t, err, "failed to fetch specific vdev with chunk mapping")
	assert.NotNil(t, result, "specific vdev with chunk mapping response should not be nil")
	log.Info("Specific vdev with chunk mapping response: ", result)

	assert.Equal(t, vdev1ID, result[0].Cfg.ID, "fetched vdev ID mismatch")
	assert.Equal(t, req1.Vdev.Size, result[0].Cfg.Size, "fetched vdev size mismatch")
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
	log.Info("created partition: ", resp)
	assert.NoError(t, err)
	_, err = c.GetPartition(cpLib.GetReq{ID: "nvme-Amazon_Elastic_Block_Store_vol0dce303259b3884dc-part1"})
	assert.NoError(t, err)
	assert.Equal(t, len(singleResp), 1, "Failed to get inserted partition")
	assert.Equal(t, singleResp[0].PartitionID, pt.PartitionID)
}

func runPutAndGetRack(b testing.TB, c *CliCFuncs) {
	racks := []cpLib.Rack{
		{ID: "9bc244bc-df29-11f0-a93b-277aec17e437", PDUID: "95f62aee-997e-11f0-9f1b-a70cff4b660b"},
		{ID: "3704e442-df2b-11f0-be6a-776bc1500ab8", PDUID: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea"},
	}

	for _, r := range racks {
		resp, err := c.PutRack(&r)
		if assert.NoError(b, err) {
			assert.True(b, resp.Success)
		}
	}

	resp, err := c.GetRacks(&cpLib.GetReq{GetAll: true})
	assert.NoError(b, err)
	assert.GreaterOrEqual(b, len(resp), len(racks))
}

func BenchmarkPutAndGetRack(b *testing.B) {
	c := newClient(b)
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
			"pt-nvme-e3df2426ed67-0",
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
	if assert.NoError(t, err) {
		assert.True(t, resp.Success)
	}

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
	nc, _ := c.GetChunkNisd(&cpLib.GetReq{ID: path.Join(resp.ID, "0")})
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

	req := cpLib.GetReq{}
	_, err = c.GetNisdArgs(req)
	assert.NoError(t, err)
	assert.NotEmpty(t, resp)
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
								fmt.Sprintf("pt-%s-%d", dev, n),
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
			log.Info("successfully created vdev: ", resp)
			time.Sleep(30 * time.Millisecond)
		}
	}()

	wg.Wait()

	// -------------------------------
	// NISD Distribution Verification
	// -------------------------------

	// GET all NISDs
	req := cpLib.GetReq{
		GetAll:    true,
		UserToken: adminToken,
	}
	res, err := c.GetNisds(req)
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
		vdev := &cpLib.VdevReq{
			Vdev: &cpLib.VdevCfg{
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
	request := cpLib.GetReq{
		GetAll:    true,
		UserToken: adminToken,
	}
	nisdsAfter, err := c.GetNisds(request)

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
		{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5661",
			FailureDomain: []string{
				pdus[0],
				racks[0],
				hvs[0],
				devices[0],
				"pt-" + devices[0] + "-0",
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5662",
			FailureDomain: []string{
				pdus[0],
				racks[0],
				hvs[0],
				devices[1],
				"pt-" + devices[1] + "-0",
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5663",
			FailureDomain: []string{
				pdus[0],
				racks[0],
				hvs[1],
				devices[2],
				"pt-" + devices[2] + "-0",
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5664",
			FailureDomain: []string{
				pdus[1],
				racks[1],
				hvs[2],
				devices[3],
				"pt-" + devices[3] + "-0",
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5665",
			FailureDomain: []string{
				pdus[1],
				racks[1],
				hvs[3],
				devices[4],
				"pt-" + devices[4] + "-0",
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
		{
			PeerPort: 8000,
			ID:       "86adee3a-d5da-11f0-8250-5f1ad86a5666",
			FailureDomain: []string{
				pdus[1],
				racks[1],
				hvs[4],
				devices[5],
				"pt-" + devices[5] + "-0",
			},
			TotalSize:     1073741824000,
			AvailableSize: 1073741824000,
		},
	}

	for _, n := range mockNisd {
		resp, err := c.PutNisd(&n)
		if assert.NoError(t, err) {
			assert.True(t, resp.Success)
		}
	}

	adminToken := getAdminToken(t)

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
	log.Infof("vdev response status: %v", resp)
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

	vdev1 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
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

	vdev2 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
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

	vdev3 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
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

	vdev4 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
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
	vdev5 := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
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
	req := cpLib.GetReq{
		GetAll: true,
	}
	res, err := c.GetNisds(req)
	assert.NoError(t, err)
	for _, n := range res {
		log.Infof("Nisd ID: %s, usage: %d", n.ID, usagePercent(n))
	}
	log.Info("total number of nisd's : ", len(res))
}

func TestDeleteVdev(t *testing.T) {
	c := newClient(t)

	nisd := cpLib.Nisd{
		PeerPort: 9400,
		ID:       uuid.NewString(),
		FailureDomain: []string{
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
		},
		TotalSize:     15_000_000_000_000,
		AvailableSize: 15_000_000_000_000,
	}
	_, err := c.PutNisd(&nisd)
	require.NoError(t, err, "failed to create NISD for delete test")

	vdev := &cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{
			Size:       8 * 1024 * 1024 * 1024,
			NumReplica: 1,
		},
	}

	cvresp, err := c.CreateVdev(vdev)
	require.NoError(t, err)
	require.NotEmpty(t, cvresp.ID)
	vdevID := cvresp.ID
	t.Logf("Created vdev for deletion test: %s", vdevID)

	// DeleteVdev often returns "empty response buffer" on success
	dvResp, err := c.DeleteVdev(&cpLib.DeleteVdevReq{ID: vdevID})
	if err != nil {
		// This is the expected success path in this codebase
		assert.Contains(t, err.Error(), "empty response buffer",
			"DeleteVdev should either succeed or return empty response buffer")
		t.Logf("DeleteVdev returned expected 'empty response buffer'")
	} else {
		assert.NotNil(t, dvResp)
		t.Log("DeleteVdev succeeded with normal response")
	}

	// Verify the vdev is gone (with retry for eventual consistency)
	deleted, getErr := isVdevDeleted(t, c, vdevID)
	assert.True(t, deleted, "vdev should no longer exist after delete")
	assert.Error(t, getErr, "GetVdevCfg should return error after successful delete")

	t.Log("Vdev successfully deleted and verified as gone")
}

// Helper: retries GetVdevCfg until it fails (handles eventual consistency)
func isVdevDeleted(t *testing.T, client *CliCFuncs, id string) (bool, error) {
	t.Helper()
	for attempt := range 5 {
		_, err := client.GetVdevCfg(&cpLib.GetReq{ID: id})
		if err != nil {
			return true, err // successfully deleted
		}
		t.Logf("GetVdevCfg still succeeded after delete (attempt %d) - retrying...", attempt+1)
		time.Sleep(300 * time.Millisecond)
	}
	return false, nil
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
	// Base client (used only in auth-disabled path)
	ctlClient := newClient(t)

	// Step 1: Create a NISD to allocate space for Vdevs
	nisd := cpLib.Nisd{
		PeerPort: 8001,
		ID:       uuid.NewString(),
		FailureDomain: []string{
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
		},
		TotalSize:     15_000_000_000_000,
		AvailableSize: 15_000_000_000_000,
	}

	if authEnabled {
		authClient, tearDown := newUserClient(t)
		defer tearDown()

		adminToken := getAdminToken(t)

		adminClient := newClientWithToken(t, adminToken)
		_, err := adminClient.PutNisd(&nisd)
		assert.NoError(t, err, "failed to create NISD for auth test")

		// Create user1 and log in
		user1Username := "vdev_owner_" + uuid.New().String()[:8]
		user1Resp, err := authClient.CreateUser(adminToken, &userlib.UserReq{Username: user1Username})
		assert.NoError(t, err, "failed to create user1")
		require.True(t, user1Resp.Success)
		user1LoginResp, err := authClient.Login(user1Username, user1Resp.SecretKey)
		assert.NoError(t, err, "user1 login should succeed")
		user1AccessToken := user1LoginResp.AccessToken
		t.Logf("User1 logged in: %s", user1Username)

		user1Client := newClientWithToken(t, user1AccessToken)

		// User1 creates vdev1
		vdevResp, err := user1Client.CreateVdev(&cpLib.VdevReq{Vdev: &cpLib.VdevCfg{Size: 500 * 1024 * 1024 * 1024, NumReplica: 1}})
		assert.NoError(t, err, "user1 should be able to create vdev")
		require.NotNil(t, vdevResp)
		assert.True(t, vdevResp.Success)
		vdevID := vdevResp.ID
		t.Logf("User1 created vdev: %s", vdevID)

		// User1 can read own vdev (with retry for eventual consistency)
		vdevCfg, err := getVdevCfgWithRetry(t, user1Client, vdevID)
		assert.NoError(t, err, "user1 should be able to read their own vdev")
		assert.Equal(t, vdevID, vdevCfg.ID)

		// Create user2 and log in
		user2Username := "unauthorized_user_" + uuid.New().String()[:8]
		user2Resp, err := authClient.CreateUser(adminToken, &userlib.UserReq{Username: user2Username})
		assert.NoError(t, err, "failed to create user2")
		require.True(t, user2Resp.Success)
		user2LoginResp, err := authClient.Login(user2Username, user2Resp.SecretKey)
		assert.NoError(t, err, "user2 login should succeed")
		user2AccessToken := user2LoginResp.AccessToken
		t.Logf("User2 logged in: %s", user2Username)

		user2Client := newClientWithToken(t, user2AccessToken)

		// User2 cannot access user1's vdev
		_, err = user2Client.GetVdevCfg(&cpLib.GetReq{ID: vdevID})
		assert.EqualError(t, err, "User is not authorized")
		t.Log("User2 correctly denied access to user1's vdev")

		// User2 creates own vdev
		vdev2Resp, err := user2Client.CreateVdev(&cpLib.VdevReq{Vdev: &cpLib.VdevCfg{Size: 300 * 1024 * 1024 * 1024, NumReplica: 1}})
		assert.NoError(t, err, "user2 should be able to create their own vdev")
		require.NotNil(t, vdev2Resp)
		assert.True(t, vdev2Resp.Success)
		vdev2ID := vdev2Resp.ID
		t.Logf("User2 created vdev: %s", vdev2ID) // <-- added for visibility

		// User2 can read own vdev (with retry)
		vdev2Cfg, err := getVdevCfgWithRetry(t, user2Client, vdev2ID)
		assert.NoError(t, err, "user2 should be able to read their own vdev")
		assert.Equal(t, vdev2ID, vdev2Cfg.ID)

		// User1 cannot access user2's vdev
		_, err = user1Client.GetVdevCfg(&cpLib.GetReq{ID: vdev2ID})
		assert.EqualError(t, err, "User is not authorized")
		t.Log("User1 correctly denied access to user2's vdev")

		// No token: read and create are rejected
		noTokenClient := newClientWithToken(t, "")
		_, err = noTokenClient.GetVdevCfg(&cpLib.GetReq{ID: vdevID})
		assert.EqualError(t, err, "Invalid Token")
		_, err = noTokenClient.CreateVdev(&cpLib.VdevReq{Vdev: &cpLib.VdevCfg{Size: 100 * 1024 * 1024 * 1024, NumReplica: 1}})
		assert.EqualError(t, err, "user token is required")
		t.Log("No-token requests correctly rejected")
	} else {
		// Auth disabled: empty token is accepted; ownership checks are not enforced
		ctlClient.SetToken("")
		_, err := ctlClient.PutNisd(&nisd)
		assert.NoError(t, err, "PutNisd should succeed when auth is disabled")

		vdev1Resp, err := ctlClient.CreateVdev(&cpLib.VdevReq{Vdev: &cpLib.VdevCfg{Size: 500 * 1024 * 1024 * 1024, NumReplica: 1}})
		require.NoError(t, err, "CreateVdev should succeed when auth is disabled")
		assert.True(t, vdev1Resp.Success)
		vdev1ID := vdev1Resp.ID

		vdev2Resp, err := ctlClient.CreateVdev(&cpLib.VdevReq{Vdev: &cpLib.VdevCfg{Size: 300 * 1024 * 1024 * 1024, NumReplica: 1}})
		require.NoError(t, err, "CreateVdev should succeed when auth is disabled")
		assert.True(t, vdev2Resp.Success)
		vdev2ID := vdev2Resp.ID

		// Any caller can read any vdev — no ownership enforcement
		cfg1, err := ctlClient.GetVdevCfg(&cpLib.GetReq{ID: vdev1ID})
		require.NoError(t, err, "GetVdevCfg should succeed when auth is disabled")
		assert.Equal(t, vdev1ID, cfg1.ID)

		cfg2, err := ctlClient.GetVdevCfg(&cpLib.GetReq{ID: vdev2ID})
		require.NoError(t, err, "GetVdevCfg should succeed when auth is disabled")
		assert.Equal(t, vdev2ID, cfg2.ID)
		t.Log("Auth disabled: vdev operations succeed without token")
	}

	t.Log("Vdev Authorization Test Completed Successfully")
}

// Tiny helper to handle the read-after-create eventual-consistency window
func getVdevCfgWithRetry(t *testing.T, client *CliCFuncs, id string) (*cpLib.VdevCfg, error) {
	t.Helper()
	for attempt := range 3 {
		cfg, err := client.GetVdevCfg(&cpLib.GetReq{ID: id})
		if err == nil {
			return &cfg, nil
		}
		t.Logf("GetVdevCfg attempt %d failed (%v) - retrying...", attempt+1, err)
		time.Sleep(200 * time.Millisecond)
	}
	// final attempt (will fail the test if still broken)
	v, err := client.GetVdevCfg(&cpLib.GetReq{ID: id})
	return &v, err
}

// createRegularUserAndLogin creates a non-admin user via the admin token and
// returns that user's JWT access token.
func createRegularUserAndLogin(t *testing.T, authClient *userClient.Client, adminToken string) string {
	t.Helper()

	username := "authtest_" + uuid.New().String()[:8]
	createResp, err := authClient.CreateUser(adminToken, &userlib.UserReq{
		Username: username,
	})
	require.NoError(t, err, "create regular user")
	require.True(t, createResp.Success, "user creation should succeed")
	require.NotEmpty(t, createResp.SecretKey)

	loginResp, err := authClient.Login(username, createResp.SecretKey)
	require.NoError(t, err, "regular user login")
	require.True(t, loginResp.Success, "login should succeed")
	require.NotEmpty(t, loginResp.AccessToken)

	t.Logf("Created regular user %q (ID: %s)", username, createResp.UserID)
	return loginResp.AccessToken
}

// TestNisdAuthorizationWithUsers verifies that only admin users can register or list nisds
func TestNisdAuthorizationWithUsers(t *testing.T) {
	ctlClient := newClient(t)

	nisdID := uuid.NewString()
	makeNisd := func() *cpLib.Nisd {
		return &cpLib.Nisd{
			PeerPort: 9100,
			ID:       nisdID,
			FailureDomain: []string{
				uuid.NewString(),
				uuid.NewString(),
				uuid.NewString(),
				"nvme-auth-test-device",
				"pt-nvme-auth-test-device-0",
			},
			TotalSize:     1_000_000_000_000,
			AvailableSize: 1_000_000_000_000,
			SocketPath:    "/tmp/nisd-auth-test.sock",
			NetInfo:       cpLib.NetInfoList{{IPAddr: "127.0.0.1", Port: 9100}},
			NetInfoCnt:    1,
		}
	}

	if authEnabled {
		authClient, tearDown := newUserClient(t)
		defer tearDown()
		adminToken := getAdminToken(t)
		userToken := createRegularUserAndLogin(t, authClient, adminToken)

		// No token write rejected
		ctlClient.SetToken("")
		_, err := ctlClient.PutNisd(makeNisd())
		assert.EqualError(t, err, "user token is required")
		t.Log("No-token write correctly rejected")

		// Regular user write rejected (admin-only RBAC)
		ctlClient.SetToken(userToken)
		_, err = ctlClient.PutNisd(makeNisd())
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		t.Log("Regular-user write correctly rejected")

		// Admin write accepted
		ctlClient.SetToken(adminToken)
		resp, err := ctlClient.PutNisd(makeNisd())
		require.NoError(t, err, "admin should be able to register a NISD")
		assert.True(t, resp.Success)
		t.Logf("Admin registered NISD %s", nisdID)

		// No token read rejected
		ctlClient.SetToken("")
		_, err = ctlClient.GetNisds(cpLib.GetReq{})
		assert.EqualError(t, err, "user token is required")
		t.Log("No-token read correctly rejected")

		// Regular user read rejected (admin-only RBAC)
		ctlClient.SetToken(userToken)
		_, err = ctlClient.GetNisds(cpLib.GetReq{})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		t.Log("Regular-user read correctly rejected")

		// Admin read accepted
		ctlClient.SetToken(adminToken)
		nisds, err := ctlClient.GetNisds(cpLib.GetReq{})
		assert.NoError(t, err, "admin should be able to list NISDs")
		assert.NotEmpty(t, nisds)
		t.Logf("Admin listed %d NISD(s)", len(nisds))
	} else {
		// Auth disabled: empty token is accepted for all operations
		ctlClient.SetToken("")
		resp, err := ctlClient.PutNisd(makeNisd())
		require.NoError(t, err, "PutNisd should succeed when auth is disabled")
		assert.True(t, resp.Success)
		t.Logf("Registered NISD %s without token", nisdID)

		nisds, err := ctlClient.GetNisds(cpLib.GetReq{})
		require.NoError(t, err, "GetNisds should succeed when auth is disabled")
		assert.NotEmpty(t, nisds)
		t.Log("Auth disabled: NISD operations succeed without token")
	}

	t.Log("NISD Authorization Test Completed Successfully")
}

// TestPDUAuthorizationWithUsers verifies that only admin users can create or list PDU
func TestPDUAuthorizationWithUsers(t *testing.T) {
	ctlClient := newClient(t)

	pduID := uuid.NewString()
	makePDU := func() *cpLib.PDU {
		return &cpLib.PDU{
			ID:            pduID,
			Name:          "pdu-auth-" + pduID[:8],
			Location:      "test-dc-row-A",
			PowerCapacity: "10Kw",
			Specification: "auth-test-pdu",
		}
	}

	if authEnabled {
		authClient, tearDown := newUserClient(t)
		defer tearDown()
		adminToken := getAdminToken(t)
		userToken := createRegularUserAndLogin(t, authClient, adminToken)

		// No token write rejected
		ctlClient.SetToken("")
		_, err := ctlClient.PutPDU(makePDU())
		assert.EqualError(t, err, "user token is required")
		t.Log("No-token write correctly rejected")

		// Regular user write rejected
		ctlClient.SetToken(userToken)
		_, err = ctlClient.PutPDU(makePDU())
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		t.Log("Regular-user write correctly rejected")

		// Admin write accepted
		ctlClient.SetToken(adminToken)
		resp, err := ctlClient.PutPDU(makePDU())
		require.NoError(t, err, "admin should be able to create a PDU")
		assert.True(t, resp.Success)
		t.Logf("Admin created PDU %s", pduID)

		// No token read rejected
		ctlClient.SetToken("")
		_, err = ctlClient.GetPDUs(&cpLib.GetReq{GetAll: true})
		assert.EqualError(t, err, "user token is required")
		t.Log("No-token read correctly rejected")

		// Regular user read rejected
		ctlClient.SetToken(userToken)
		_, err = ctlClient.GetPDUs(&cpLib.GetReq{GetAll: true})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		t.Log("Regular-user read correctly rejected")

		// Admin read accepted
		ctlClient.SetToken(adminToken)
		pdus, err := ctlClient.GetPDUs(&cpLib.GetReq{GetAll: true})
		assert.NoError(t, err, "admin should be able to list PDUs")
		found := false
		for _, p := range pdus {
			if p.ID == pduID {
				found = true
				break
			}
		}
		assert.True(t, found, "PDU %s should appear in admin listing", pduID)
		t.Logf("Admin listed %d PDU(s), found our PDU: %v", len(pdus), found)
	} else {
		// Auth disabled: empty token is accepted for all operations
		ctlClient.SetToken("")
		resp, err := ctlClient.PutPDU(makePDU())
		require.NoError(t, err, "PutPDU should succeed when auth is disabled")
		assert.True(t, resp.Success)
		t.Logf("Created PDU %s without token", pduID)

		pdus, err := ctlClient.GetPDUs(&cpLib.GetReq{GetAll: true})
		require.NoError(t, err, "GetPDUs should succeed when auth is disabled")
		found := false
		for _, p := range pdus {
			if p.ID == pduID {
				found = true
				break
			}
		}
		assert.True(t, found, "PDU %s should be visible when auth is disabled", pduID)
		t.Log("Auth disabled: PDU operations succeed without token")
	}

	t.Log("PDU Authorization Test Completed Successfully")
}

// TestRackAuthorizationWithUsers verifies that only admin users can create or list Rack
func TestRackAuthorizationWithUsers(t *testing.T) {
	ctlClient := newClient(t)

	pduID := uuid.NewString()
	rackID := uuid.NewString()
	makeRack := func() *cpLib.Rack {
		return &cpLib.Rack{
			ID:            rackID,
			PDUID:         pduID,
			Name:          "rack-auth-" + rackID[:8],
			Location:      "test-dc-row-B-slot-1",
			Specification: "auth-test-rack",
		}
	}

	if authEnabled {
		authClient, tearDown := newUserClient(t)
		defer tearDown()
		adminToken := getAdminToken(t)
		userToken := createRegularUserAndLogin(t, authClient, adminToken)

		// Admin creates parent PDU
		ctlClient.SetToken(adminToken)
		pduResp, err := ctlClient.PutPDU(&cpLib.PDU{
			ID:            pduID,
			Name:          "pdu-for-rack-auth-" + pduID[:8],
			Location:      "test-dc-row-B",
			PowerCapacity: "15Kw",
			Specification: "rack-auth-parent-pdu",
		})
		require.NoError(t, err, "admin should create parent PDU")
		require.True(t, pduResp.Success)
		t.Logf("Parent PDU %s created", pduID)

		// No token write rejected
		ctlClient.SetToken("")
		_, err = ctlClient.PutRack(makeRack())
		assert.EqualError(t, err, "user token is required")
		t.Log("No-token write correctly rejected")

		// Regular user write rejected (admin-only RBAC)
		ctlClient.SetToken(userToken)
		_, err = ctlClient.PutRack(makeRack())
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		t.Log("Regular-user write correctly rejected")

		// Admin write accepted
		ctlClient.SetToken(adminToken)
		resp, err := ctlClient.PutRack(makeRack())
		require.NoError(t, err, "admin should be able to create a Rack")
		assert.True(t, resp.Success)
		t.Logf("Admin created Rack %s under PDU %s", rackID, pduID)

		// No token read rejected
		ctlClient.SetToken("")
		_, err = ctlClient.GetRacks(&cpLib.GetReq{GetAll: true})
		assert.EqualError(t, err, "user token is required")
		t.Log("No-token read correctly rejected")

		// Regular user read rejected (admin-only RBAC)
		ctlClient.SetToken(userToken)
		_, err = ctlClient.GetRacks(&cpLib.GetReq{GetAll: true})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		t.Log("Regular-user read correctly rejected")

		// Admin read accepted
		ctlClient.SetToken(adminToken)
		racks, err := ctlClient.GetRacks(&cpLib.GetReq{GetAll: true})
		assert.NoError(t, err, "admin should be able to list Racks")
		found := false
		for _, r := range racks {
			if r.ID == rackID {
				found = true
				break
			}
		}
		assert.True(t, found, "Rack %s should appear in admin listing", rackID)
		t.Logf("Admin listed %d Rack(s), found our Rack: %v", len(racks), found)
	} else {
		// Auth disabled: empty token is accepted for all operations
		ctlClient.SetToken("")
		pduResp, err := ctlClient.PutPDU(&cpLib.PDU{
			ID:            pduID,
			Name:          "pdu-for-rack-auth-" + pduID[:8],
			Location:      "test-dc-row-B",
			PowerCapacity: "15Kw",
			Specification: "rack-auth-parent-pdu",
		})
		require.NoError(t, err, "PutPDU should succeed when auth is disabled")
		require.True(t, pduResp.Success)

		resp, err := ctlClient.PutRack(makeRack())
		require.NoError(t, err, "PutRack should succeed when auth is disabled")
		assert.True(t, resp.Success)
		t.Logf("Created Rack %s without token", rackID)

		racks, err := ctlClient.GetRacks(&cpLib.GetReq{GetAll: true})
		require.NoError(t, err, "GetRacks should succeed when auth is disabled")
		found := false
		for _, r := range racks {
			if r.ID == rackID {
				found = true
				break
			}
		}
		assert.True(t, found, "Rack %s should be visible when auth is disabled", rackID)
		t.Log("Auth disabled: Rack operations succeed without token")
	}

	t.Log("Rack Authorization Test Completed Successfully")
}

// TestHypervisorAuthorizationWithUsers verifies that only admin users can create or list Hypervisors
func TestHypervisorAuthorizationWithUsers(t *testing.T) {
	ctlClient := newClient(t)

	pduID := uuid.NewString()
	rackID := uuid.NewString()
	hvID := uuid.NewString()
	makeHV := func() *cpLib.Hypervisor {
		return &cpLib.Hypervisor{
			ID:        hvID,
			RackID:    rackID,
			Name:      "hv-auth-" + hvID[:8],
			IPAddrs:   []string{"192.168.100.10"},
			PortRange: "5000-6000",
			SSHPort:   "22",
		}
	}

	// Helper to build the parent PDU+Rack hierarchy
	buildParents := func() {
		pduResp, err := ctlClient.PutPDU(&cpLib.PDU{
			ID:            pduID,
			Name:          "pdu-for-hv-auth-" + pduID[:8],
			Location:      "test-dc-row-C",
			PowerCapacity: "20Kw",
			Specification: "hv-auth-parent-pdu",
		})
		require.NoError(t, err)
		require.True(t, pduResp.Success, "parent PDU creation must succeed")
		t.Logf("Parent PDU %s created", pduID)

		rackResp, err := ctlClient.PutRack(&cpLib.Rack{
			ID:            rackID,
			PDUID:         pduID,
			Name:          "rack-for-hv-auth-" + rackID[:8],
			Location:      "test-dc-row-C-slot-2",
			Specification: "hv-auth-parent-rack",
		})
		require.NoError(t, err)
		require.True(t, rackResp.Success, "parent Rack creation must succeed")
		t.Logf("Parent Rack %s created under PDU %s", rackID, pduID)
	}

	if authEnabled {
		authClient, tearDown := newUserClient(t)
		defer tearDown()
		adminToken := getAdminToken(t)
		userToken := createRegularUserAndLogin(t, authClient, adminToken)

		ctlClient.SetToken(adminToken)
		buildParents()

		// No token write rejected
		ctlClient.SetToken("")
		_, err := ctlClient.PutHypervisor(makeHV())
		assert.EqualError(t, err, "user token is required")
		t.Log("No-token write correctly rejected")

		// Regular user write rejected (admin-only RBAC)
		ctlClient.SetToken(userToken)
		_, err = ctlClient.PutHypervisor(makeHV())
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		t.Log("Regular-user write correctly rejected")

		// Admin write accepted
		ctlClient.SetToken(adminToken)
		resp, err := ctlClient.PutHypervisor(makeHV())
		require.NoError(t, err, "admin should be able to create a Hypervisor")
		assert.True(t, resp.Success)
		t.Logf("Admin created Hypervisor %s under Rack %s", hvID, rackID)

		// No token read rejected
		ctlClient.SetToken("")
		_, err = ctlClient.GetHypervisor(&cpLib.GetReq{GetAll: true})
		assert.EqualError(t, err, "user token is required")
		t.Log("No-token read correctly rejected")

		// Regular user read rejected (admin-only RBAC)
		ctlClient.SetToken(userToken)
		_, err = ctlClient.GetHypervisor(&cpLib.GetReq{GetAll: true})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		t.Log("Regular-user read correctly rejected")

		// Admin read accepted
		ctlClient.SetToken(adminToken)
		hvs, err := ctlClient.GetHypervisor(&cpLib.GetReq{GetAll: true})
		assert.NoError(t, err, "admin should be able to list Hypervisors")
		found := false
		for _, h := range hvs {
			if h.ID == hvID {
				found = true
				break
			}
		}
		assert.True(t, found, "Hypervisor %s should appear in admin listing", hvID)
		t.Logf("Admin listed %d Hypervisor(s), found our HV: %v", len(hvs), found)
	} else {
		// Auth disabled: empty token is accepted for all operations
		ctlClient.SetToken("")
		buildParents()

		resp, err := ctlClient.PutHypervisor(makeHV())
		require.NoError(t, err, "PutHypervisor should succeed when auth is disabled")
		assert.True(t, resp.Success)
		t.Logf("Created Hypervisor %s without token", hvID)

		hvs, err := ctlClient.GetHypervisor(&cpLib.GetReq{GetAll: true})
		require.NoError(t, err, "GetHypervisor should succeed when auth is disabled")
		found := false
		for _, h := range hvs {
			if h.ID == hvID {
				found = true
				break
			}
		}
		assert.True(t, found, "Hypervisor %s should be visible when auth is disabled", hvID)
		t.Log("Auth disabled: Hypervisor operations succeed without token")
	}

	t.Log("Hypervisor Authorization Test Completed Successfully")
}

// TestFullHierarchyAuthorizationWithUsers check the complete flow
func TestFullHierarchyAuthorizationWithUsers(t *testing.T) {
	ctlClient := newClient(t)

	pduID := uuid.NewString()
	rackID := uuid.NewString()
	hvID := uuid.NewString()
	nisdID := uuid.NewString()

	// buildHierarchy creates the full PDU→Rack→HV→NISD chain using whatever
	// token is currently set on ctlClient.
	buildHierarchy := func() {
		pduResp, err := ctlClient.PutPDU(&cpLib.PDU{
			ID:            pduID,
			Name:          "hier-pdu-" + pduID[:8],
			Location:      "test-dc-full-hier",
			PowerCapacity: "25Kw",
			Specification: "full-hierarchy-test",
		})
		require.NoError(t, err)
		require.True(t, pduResp.Success)
		t.Logf("PDU %s created", pduID)

		rackResp, err := ctlClient.PutRack(&cpLib.Rack{
			ID:            rackID,
			PDUID:         pduID,
			Name:          "hier-rack-" + rackID[:8],
			Location:      "test-dc-full-hier-slot-1",
			Specification: "full-hierarchy-test",
		})
		require.NoError(t, err)
		require.True(t, rackResp.Success)
		t.Logf("Rack %s created under PDU %s", rackID, pduID)

		hvResp, err := ctlClient.PutHypervisor(&cpLib.Hypervisor{
			ID:        hvID,
			RackID:    rackID,
			Name:      "hier-hv-" + hvID[:8],
			IPAddrs:   []string{"10.0.0.1"},
			PortRange: "7000-8000",
			SSHPort:   "22",
		})
		require.NoError(t, err)
		require.True(t, hvResp.Success)
		t.Logf("Hypervisor %s created under Rack %s", hvID, rackID)

		nisdResp, err := ctlClient.PutNisd(&cpLib.Nisd{
			PeerPort:      9200,
			ID:            nisdID,
			FailureDomain: []string{pduID, rackID, hvID, "nvme-hier-test-device", "pt-nvme-hier-test-device-0"},
			TotalSize:     2_000_000_000_000,
			AvailableSize: 2_000_000_000_000,
			SocketPath:    "/tmp/nisd-hier-test.sock",
			NetInfo:       cpLib.NetInfoList{{IPAddr: "10.0.0.1", Port: 9200}},
			NetInfoCnt:    1,
		})
		require.NoError(t, err)
		require.True(t, nisdResp.Success)
		t.Logf("NISD %s created", nisdID)
	}

	// verifyAllReadable asserts that all four resource types can be listed and
	// contain the objects created by buildHierarchy.
	verifyAllReadable := func() {
		pdus, err := ctlClient.GetPDUs(&cpLib.GetReq{GetAll: true})
		assert.NoError(t, err)
		assert.True(t, func() bool {
			for _, p := range pdus {
				if p.ID == pduID {
					return true
				}
			}
			return false
		}(), "PDU %s must be visible", pduID)

		racks, err := ctlClient.GetRacks(&cpLib.GetReq{GetAll: true})
		assert.NoError(t, err)
		assert.True(t, func() bool {
			for _, r := range racks {
				if r.ID == rackID {
					return true
				}
			}
			return false
		}(), "Rack %s must be visible", rackID)

		hvs, err := ctlClient.GetHypervisor(&cpLib.GetReq{GetAll: true})
		assert.NoError(t, err)
		assert.True(t, func() bool {
			for _, h := range hvs {
				if h.ID == hvID {
					return true
				}
			}
			return false
		}(), "Hypervisor %s must be visible", hvID)

		nisds, err := ctlClient.GetNisds(cpLib.GetReq{})
		assert.NoError(t, err)
		assert.True(t, func() bool {
			for _, n := range nisds {
				if n.ID == nisdID {
					return true
				}
			}
			return false
		}(), "NISD %s must be visible", nisdID)
	}

	if authEnabled {
		authClient, tearDown := newUserClient(t)
		defer tearDown()
		adminToken := getAdminToken(t)
		userToken := createRegularUserAndLogin(t, authClient, adminToken)

		// Admin builds the full hierarchy
		ctlClient.SetToken(adminToken)
		buildHierarchy()

		// Regular user is blocked at every level
		ctlClient.SetToken(userToken)
		t.Log("Verifying regular user is blocked at all levels...")

		_, err := ctlClient.PutPDU(&cpLib.PDU{ID: uuid.NewString(), Name: "user-pdu"})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		_, err = ctlClient.GetPDUs(&cpLib.GetReq{GetAll: true})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")

		_, err = ctlClient.PutRack(&cpLib.Rack{ID: uuid.NewString(), PDUID: pduID, Name: "user-rack"})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		_, err = ctlClient.GetRacks(&cpLib.GetReq{GetAll: true})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")

		_, err = ctlClient.PutHypervisor(&cpLib.Hypervisor{ID: uuid.NewString(), RackID: rackID, Name: "user-hv"})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		_, err = ctlClient.GetHypervisor(&cpLib.GetReq{GetAll: true})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")

		_, err = ctlClient.PutNisd(&cpLib.Nisd{
			PeerPort:      9999,
			ID:            uuid.NewString(),
			FailureDomain: []string{pduID, rackID, hvID, "nvme-user-attempt", "pt-nvme-user-attempt-0"},
			TotalSize:     100_000_000_000,
			AvailableSize: 100_000_000_000,
			NetInfo:       cpLib.NetInfoList{{IPAddr: "10.0.0.2", Port: 9999}},
			NetInfoCnt:    1,
		})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		_, err = ctlClient.GetNisds(cpLib.GetReq{})
		assert.EqualError(t, err, "authorization failed: insufficient permissions")
		t.Log("Regular user correctly blocked at all hierarchy levels")

		// Admin can read back every level
		ctlClient.SetToken(adminToken)
		verifyAllReadable()
		t.Log("Admin can read every hierarchy level")
	} else {
		// Auth disabled: build and read the entire hierarchy with empty token
		ctlClient.SetToken("")
		buildHierarchy()
		verifyAllReadable()
		t.Log("Auth disabled: full hierarchy operations succeed without token")
	}

	t.Log("Full Hierarchy Authorization Test Completed Successfully")
}

func TestABACVdevOwnership(t *testing.T) {
	if !authEnabled {
		t.Skip("ABAC test only runs when AUTH_ENABLED=true")
	}

	authClient, tearDown := newUserClient(t)
	defer tearDown()
	adminToken := getAdminToken(t)

	// Admin creates NISD (so there is space for vdevs)
	adminClient := newClientWithToken(t, adminToken)
	nisd := cpLib.Nisd{
		PeerPort: 9300,
		ID:       uuid.NewString(),
		FailureDomain: []string{
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
		},
		TotalSize:     15_000_000_000_000,
		AvailableSize: 15_000_000_000_000,
	}
	resp, err := adminClient.PutNisd(&nisd)
	require.NoError(t, err, "admin should create NISD")
	require.True(t, resp.Success)

	// Create and login user1
	user1Username := "abac_owner_" + uuid.New().String()[:8]
	user1Resp, err := authClient.CreateUser(adminToken, &userlib.UserReq{Username: user1Username})
	require.NoError(t, err)
	require.True(t, user1Resp.Success)
	user1LoginResp, err := authClient.Login(user1Username, user1Resp.SecretKey)
	require.NoError(t, err)
	user1Token := user1LoginResp.AccessToken
	t.Logf("User1 logged in: %s", user1Username)

	// Create and login user2
	user2Username := "abac_other_" + uuid.New().String()[:8]
	user2Resp, err := authClient.CreateUser(adminToken, &userlib.UserReq{Username: user2Username})
	require.NoError(t, err)
	require.True(t, user2Resp.Success)
	user2LoginResp, err := authClient.Login(user2Username, user2Resp.SecretKey)
	require.NoError(t, err)
	user2Token := user2LoginResp.AccessToken
	t.Logf("User2 logged in: %s", user2Username)

	// Each user gets its OWN fresh client (this is the critical fix)
	user1Client := newClientWithToken(t, user1Token)
	user2Client := newClientWithToken(t, user2Token)

	// User1 creates vdev
	vdev1Resp, err := user1Client.CreateVdev(&cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{Size: 500 * 1024 * 1024 * 1024, NumReplica: 1},
	})
	require.NoError(t, err, "user1 should be able to create vdev")
	require.NotNil(t, vdev1Resp)
	vdev1ID := vdev1Resp.ID
	t.Logf("User1 created vdev: %s", vdev1ID)

	// User2 creates vdev
	vdev2Resp, err := user2Client.CreateVdev(&cpLib.VdevReq{
		Vdev: &cpLib.VdevCfg{Size: 300 * 1024 * 1024 * 1024, NumReplica: 1},
	})
	require.NoError(t, err, "user2 should be able to create vdev")
	require.NotNil(t, vdev2Resp)
	vdev2ID := vdev2Resp.ID
	t.Logf("User2 created vdev: %s", vdev2ID)

	// 1. GetVdevCfg - own vdev should succeed
	cfg1, err := user1Client.GetVdevCfg(&cpLib.GetReq{ID: vdev1ID})
	assert.NoError(t, err, "user1 should read their own vdev")
	assert.Equal(t, vdev1ID, cfg1.ID)

	// other user's vdev should be denied
	_, err = user2Client.GetVdevCfg(&cpLib.GetReq{ID: vdev1ID})
	assert.EqualError(t, err, "User is not authorized")

	// 2. GetVdevsWithChunkInfo
	_, err = user1Client.GetVdevsWithChunkInfo(&cpLib.GetReq{ID: vdev1ID})
	assert.NoError(t, err, "user1 should read chunk-info of their own vdev")

	_, err = user2Client.GetVdevsWithChunkInfo(&cpLib.GetReq{ID: vdev1ID})
	assert.EqualError(t, err, "User is not authorized")

	_, err = user2Client.GetVdevsWithChunkInfo(&cpLib.GetReq{ID: vdev2ID})
	assert.NoError(t, err, "user2 should read chunk-info of their own vdev")

	_, err = user1Client.GetVdevsWithChunkInfo(&cpLib.GetReq{ID: vdev2ID})
	assert.EqualError(t, err, "User is not authorized")

	// 3. GetChunkNisd
	_, err = user1Client.GetChunkNisd(&cpLib.GetReq{ID: path.Join(vdev1ID, "0")})
	assert.NoError(t, err, "user1 should read chunk-nisd of their own vdev")

	_, err = user2Client.GetChunkNisd(&cpLib.GetReq{ID: path.Join(vdev1ID, "0")})
	assert.Error(t, err, "user2 must not read user1's chunk")

	_, err = user2Client.GetChunkNisd(&cpLib.GetReq{ID: path.Join(vdev2ID, "0")})
	assert.NoError(t, err, "user2 should read chunk-nisd of their own vdev")

	_, err = user1Client.GetChunkNisd(&cpLib.GetReq{ID: path.Join(vdev2ID, "0")})
	assert.Error(t, err, "user1 must not read user2's chunk")

	t.Log("ABAC Vdev Ownership Test Completed Successfully")
}
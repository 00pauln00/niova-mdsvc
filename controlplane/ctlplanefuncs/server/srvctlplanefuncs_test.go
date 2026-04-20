package srvctlplanefuncs

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"

	auth "github.com/00pauln00/niova-mdsvc/controlplane/auth/jwt"
	authz "github.com/00pauln00/niova-mdsvc/controlplane/authorizer"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"

	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
	"github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/memstore"
)

type testDataStore struct {
	data map[string]string
}

func newTestDataStore() *testDataStore {
	return &testDataStore{data: make(map[string]string)}
}

func (s *testDataStore) Read(key, selector string) ([]byte, error) {
	val, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return []byte(val), nil
}

func (s *testDataStore) Write(key, value, selector string) error {
	s.data[key] = value
	return nil
}

func (s *testDataStore) Delete(key, selector string) error {
	delete(s.data, key)
	return nil
}

// RangeRead returns all keys whose string representation starts with args.Prefix.
func (s *testDataStore) RangeRead(args storageiface.RangeReadArgs) (*storageiface.RangeReadResult, error) {
	result := &storageiface.RangeReadResult{
		ResultMap: make(map[string][]byte),
	}
	for k, v := range s.data {
		if strings.HasPrefix(k, args.Prefix) {
			result.ResultMap[k] = []byte(v)
		}
	}
	return result, nil
}

const (
	testPDU  = "acdef556-1ea3-11f1-848b-9f6e716afc46"
	testRack = "b1b89a50-1ea3-11f1-b397-d76191bdb3d2"
	testHV   = "b726b99a-1ea3-11f1-95da-436ff27bf77e"
	testDev  = "nvme-001"
	testPT   = "nvme-001-01"
)

// TestMain initializes the test environment
func TestMain(m *testing.M) {
	// Initialize xlog to prevent nil pointer errors
	logLevel := "INFO"
	log.InitXlog("/tmp/test.log", &logLevel)

	ctlplfl.RegisterGOBStructs()

	// Run tests
	code := m.Run()

	os.Exit(code)
}

// Test constants
const (
	testUserID1 = "user-123"
	testUserID2 = "user-456"
	testAdminID = "admin-001"
	testVdevID  = "vdev-test-001"
)

var (
	testSecret = []byte(ctlplfl.CP_SECRET)
)

// Helper function to create valid JWT token
func createTestToken(userID, role string, secret []byte) (string, error) {
	tc := &auth.Token{
		Secret: secret,
		TTL:    time.Hour, // Valid for 1 hour
	}
	claims := map[string]any{
		"userID": userID,
		"role":   role,
	}
	return tc.CreateToken(claims)
}

/*
// Helper function to create expired token
func createExpiredToken(userID, role string, secret []byte) (string, error) {
	tc := &auth.Token{
		Secret: secret,
		TTL:    -time.Hour, // Already expired
	}
	claims := map[string]any{
		"userID": userID,
		"role":   role,
	}
	return tc.CreateToken(claims)
}
*/

/*
// Helper function to verify ownership key exists in datastore
func verifyOwnershipKey(ds storageiface.DataStore, userID, vdevID string) bool {
	ownershipKey := fmt.Sprintf("/u/%s/v/%s", userID, vdevID)
	result, err := ds.Read(ownershipKey, "")
	if err != nil {
		return false
	}
	return string(result) == "1"
}
*/

// Helper function to setup vdev data in memstore
func setupVdevData(ds storageiface.DataStore, vdevID string) error {
	vdevKey := fmt.Sprintf("v/%s", vdevID)

	// Write vdev configuration data
	err := ds.Write(fmt.Sprintf("%s/cfg/size", vdevKey), "1073741824", "")
	if err != nil {
		return err
	}
	err = ds.Write(fmt.Sprintf("%s/cfg/num_chunks", vdevKey), "4", "")
	if err != nil {
		return err
	}
	err = ds.Write(fmt.Sprintf("%s/cfg/num_replicas", vdevKey), "3", "")
	if err != nil {
		return err
	}
	return nil
}

func TestWPCreateVdev(t *testing.T) {
	// Initialize test authorizer
	authorizer = authz.NewAuthorizerWithConfig(authz.Config{
		authz.WPCreateVdev: authz.FunctionPolicy{
			RBAC: []string{"admin", "user"},
		},
	})
	defer func() { authorizer = nil }()

	testCases := []struct {
		name            string
		setupToken      func() string
		vdevSize        int64
		numChunks       int
		numReplica      int
		expectError     bool
		errorContains   string
		expectedErrCode ctlplfl.CPErrCode
		checkOwnership  bool
		expectedUserID  string
	}{
		{
			name: "SuccessfulCreation_UserRole",
			setupToken: func() string {
				token, _ := createTestToken(testUserID1, "user", testSecret)
				return token
			},
			vdevSize:       1073741824,
			numChunks:      4,
			numReplica:     3,
			expectError:    false,
			checkOwnership: true,
			expectedUserID: testUserID1,
		},
		{
			name: "SuccessfulCreation_AdminRole",
			setupToken: func() string {
				token, _ := createTestToken(testAdminID, "admin", testSecret)
				return token
			},
			vdevSize:       1073741824,
			numChunks:      4,
			numReplica:     3,
			expectError:    false,
			checkOwnership: true,
			expectedUserID: testAdminID,
		},
		{
			name: "MissingToken",
			setupToken: func() string {
				return ""
			},
			vdevSize:        1073741824,
			numChunks:       4,
			numReplica:      3,
			expectError:     true,
			errorContains:   "user token is required",
			expectedErrCode: ctlplfl.ErrAuth,
		},
		{
			name: "MissingUserIDClaim",
			setupToken: func() string {
				tc := &auth.Token{Secret: testSecret, TTL: time.Hour}
				claims := map[string]any{"role": "user"} // Missing userID
				token, _ := tc.CreateToken(claims)
				return token
			},
			vdevSize:        1073741824,
			numChunks:       4,
			numReplica:      3,
			expectError:     true,
			errorContains:   "missing userID",
			expectedErrCode: ctlplfl.ErrAuth,
		},
		{
			name: "MissingRoleClaim",
			setupToken: func() string {
				tc := &auth.Token{Secret: testSecret, TTL: time.Hour}
				claims := map[string]any{"userID": testUserID1} // Missing role
				token, _ := tc.CreateToken(claims)
				return token
			},
			vdevSize:        1073741824,
			numChunks:       4,
			numReplica:      3,
			expectError:     true,
			errorContains:   "missing role",
			expectedErrCode: ctlplfl.ErrAuth,
		},
		{
			name: "UnauthorizedRole",
			setupToken: func() string {
				token, _ := createTestToken(testUserID1, "viewer", testSecret)
				return token
			},
			vdevSize:        1073741824,
			numChunks:       4,
			numReplica:      3,
			expectError:     true,
			errorContains:   "authorization failed",
			expectedErrCode: ctlplfl.ErrAuth,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test vdev
			cpReq := ctlplfl.CPReq{
				Token: tc.setupToken(),
				Payload: ctlplfl.VdevReq{
					Vdev: &ctlplfl.VdevCfg{
						Size:       tc.vdevSize,
						NumChunks:  uint32(tc.numChunks),
						NumReplica: uint8(tc.numReplica),
					},
				},
			}

			// Call WPCreateVdev
			result, err := WPCreateVdev(cpReq)

			// Check error expectations
			if tc.expectError {
				if result == nil {
					t.Error("Expected non-nil result for encoded error response")
					return
				}
				// WP functions return FuncIntrm; decode it first, then extract CPResp.
				var intrm funclib.FuncIntrm
				if decErr := pmCmn.Decoder(pmCmn.GOB, result.([]byte), &intrm); decErr != nil {
					t.Fatalf("Failed to decode FuncIntrm: %v", decErr)
				}
				cpResp, ok := intrm.Response.(ctlplfl.CPResp)
				if !ok || cpResp.Error == nil {
					t.Errorf("Expected CPResp with error in FuncIntrm.Response, got %T", intrm.Response)
					return
				}
				if tc.errorContains != "" && !strings.Contains(cpResp.Error.Message, tc.errorContains) {
					t.Errorf("Expected error message containing %q, got %q", tc.errorContains, cpResp.Error.Message)
				}
				if tc.expectedErrCode != "" && cpResp.Error.Code != tc.expectedErrCode {
					t.Errorf("Expected error code %q, got %q", tc.expectedErrCode, cpResp.Error.Code)
				}
				return
			}

			// Check success case
			if err != nil {
				t.Errorf("Expected no error, but got: %v", err)
				return
			}

			if result == nil {
				t.Error("Expected non-nil result")
				return
			}

			// Decode the result to verify structure
			var funcIntrm funclib.FuncIntrm
			err = pmCmn.Decoder(pmCmn.GOB, result.([]byte), &funcIntrm)
			if err != nil {
				t.Errorf("Failed to decode result: %v", err)
				return
			}

			// Verify ownership key in commit changes if needed
			if tc.checkOwnership {
				var foundOwnership bool
				expectedOwnershipKey := fmt.Sprintf("/u/%s/v/", tc.expectedUserID)

				for _, chg := range funcIntrm.Changes {
					key := string(chg.Key)
					if strings.Contains(key, expectedOwnershipKey) && string(chg.Value) == "1" {
						foundOwnership = true
						break
					}
				}

				if !foundOwnership {
					t.Errorf("Expected ownership key for user %s, but not found in commit changes", tc.expectedUserID)
				}
			}
		})
	}
}

func TestWPCreateVdev_NilAuthorizer(t *testing.T) {
	// Ensure authorizer is nil
	authorizer = nil
	defer func() { authorizer = nil }()

	token, _ := createTestToken(testUserID1, "user", testSecret)
	cpReq := ctlplfl.CPReq{
		Token: token,
		Payload: ctlplfl.VdevReq{
			Vdev: &ctlplfl.VdevCfg{
				Size:       1073741824,
				NumChunks:  4,
				NumReplica: 3,
			},
		},
	}

	// Should succeed even without authorizer (graceful degradation)
	result, err := WPCreateVdev(cpReq)
	if err != nil {
		t.Errorf("Expected success with nil authorizer, but got error: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestReadVdevInfo(t *testing.T) {
	// Initialize test authorizer
	authorizer = authz.NewAuthorizerWithConfig(authz.Config{
		authz.ReadVdevInfo: authz.FunctionPolicy{
			RBAC: []string{"admin", "user"},
			ABAC: []authz.ABACRule{
				{Argument: "vdev", Prefix: "v/"},
			},
		},
	})
	defer func() { authorizer = nil }()

	testCases := []struct {
		name            string
		setupData       func(storageiface.DataStore)
		setupToken      func() string
		vdevID          string
		expectError     bool
		errorContains   string
		expectedErrCode ctlplfl.CPErrCode
	}{
		{
			name: "SuccessfulRead_Owner",
			setupData: func(ds storageiface.DataStore) {
				// Setup vdev data
				setupVdevData(ds, testVdevID)
				// Setup ownership key
				ownershipKey := fmt.Sprintf("/u/%s/v/%s", testUserID1, testVdevID)
				ds.Write(ownershipKey, "1", "")
			},
			setupToken: func() string {
				token, _ := createTestToken(testUserID1, "user", testSecret)
				return token
			},
			vdevID:      testVdevID,
			expectError: false,
		},
		{
			name: "MissingToken",
			setupData: func(ds storageiface.DataStore) {
				setupVdevData(ds, testVdevID)
			},
			setupToken: func() string {
				return ""
			},
			vdevID:          testVdevID,
			expectError:     true,
			errorContains:   "Invalid Token",
			expectedErrCode: ctlplfl.ErrAuth,
		},
		{
			name: "UnauthorizedRole_RBAC",
			setupData: func(ds storageiface.DataStore) {
				setupVdevData(ds, testVdevID)
			},
			setupToken: func() string {
				token, _ := createTestToken(testUserID1, "viewer", testSecret)
				return token
			},
			vdevID:          testVdevID,
			expectError:     true,
			errorContains:   "User is not authorized",
			expectedErrCode: ctlplfl.ErrAuth,
		},
		{
			name: "UnauthorizedUser_NotOwner",
			setupData: func(ds storageiface.DataStore) {
				setupVdevData(ds, testVdevID)
				// Setup ownership for user2, but we'll try to access as user1
				ownershipKey := fmt.Sprintf("/u/%s/v/%s", testUserID2, testVdevID)
				ds.Write(ownershipKey, "1", "")
			},
			setupToken: func() string {
				token, _ := createTestToken(testUserID1, "user", testSecret)
				return token
			},
			vdevID:          testVdevID,
			expectError:     true,
			errorContains:   "User is not authorized",
			expectedErrCode: ctlplfl.ErrAuth,
		},
		{
			name: "InvalidRequest_EmptyID",
			setupData: func(ds storageiface.DataStore) {
				// No setup needed
			},
			setupToken: func() string {
				token, _ := createTestToken(testUserID1, "user", testSecret)
				return token
			},
			vdevID:          "",
			expectError:     true,
			errorContains:   "Invalid Request",
			expectedErrCode: ctlplfl.ErrFunc,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create memstore and setup test data
			ds := memstore.NewMemStore()
			colmfamily = "" // Use default column family

			if tc.setupData != nil {
				tc.setupData(ds)
			}

			// Create mock PmdbCbArgs
			cbArgs := &PumiceDBServer.PmdbCbArgs{
				Store:     ds,
				ReplySize: 4096,
			}

			// Build CPReq with GetReq as payload
			req := ctlplfl.GetReq{
				ID:     tc.vdevID,
				GetAll: false,
			}
			cpReq := ctlplfl.CPReq{
				Token:   tc.setupToken(),
				Payload: req,
			}

			// Call ReadVdevInfo
			result, err := ReadVdevInfo(cbArgs, cpReq)

			// Check error expectations
			if tc.expectError {
				if result == nil {
					t.Error("Expected non-nil result for encoded error response")
					return
				}
				var cpResp ctlplfl.CPResp
				if decErr := pmCmn.Decoder(pmCmn.GOB, result.([]byte), &cpResp); decErr != nil {
					t.Fatalf("Failed to decode response: %v", decErr)
				}
				if cpResp.Error == nil {
					t.Errorf("Expected error response, got success")
					return
				}
				if tc.errorContains != "" && !strings.Contains(cpResp.Error.Message, tc.errorContains) {
					t.Errorf("Expected error message containing %q, got %q", tc.errorContains, cpResp.Error.Message)
				}
				if tc.expectedErrCode != "" && cpResp.Error.Code != tc.expectedErrCode {
					t.Errorf("Expected error code %q, got %q", tc.expectedErrCode, cpResp.Error.Code)
				}
				return
			}

			// Check success case
			if err != nil {
				t.Errorf("Expected no error, but got: %v", err)
				return
			}

			if result == nil {
				t.Error("Expected non-nil result")
				return
			}

			var cpResp ctlplfl.CPResp
			if decErr := pmCmn.Decoder(pmCmn.GOB, result.([]byte), &cpResp); decErr != nil {
				t.Fatalf("Failed to decode response: %v", decErr)
			}
			if cpResp.Error != nil {
				t.Errorf("Expected success response, got error: %s", cpResp.Err())
			}
		})
	}
}

func TestAPDeleteVdev(t *testing.T) {

	t.Log("Starting TestAPDeleteVdev")

	testVdevUUID := "28061cd0-1e01-11f1-a069-032bff036f03"
	testNisdUUID := "59ee0460-1e01-11f1-9566-83949aa998ea"

	testCases := []struct {
		name          string
		setupData     func(storageiface.DataStore)
		setupHR       func()
		vdevID        string
		expectError   bool
		errorContains string
		verify        func(t *testing.T, ds storageiface.DataStore)
	}{
		{
			name: "SuccessfulDelete_WithChunks",

			setupData: func(ds storageiface.DataStore) {
				t.Log("Setting up datastore for SuccessfulDelete_WithChunks")

				ds.Write(fmt.Sprintf("v/%s/cfg/size", testVdevUUID), "8589934592", "")
				ds.Write(fmt.Sprintf("v/%s/c/0/R.0", testVdevUUID), testNisdUUID, "")
				ds.Write(fmt.Sprintf("n/%s/%s", testNisdUUID, testVdevUUID), "R.0.0", "")

				// n_cfg entries similar to logs
				ds.Write(fmt.Sprintf("n_cfg/%s/d", testNisdUUID), testDev, "")
				ds.Write(fmt.Sprintf("n_cfg/%s/pp", testNisdUUID), "8160", "")
				ds.Write(fmt.Sprintf("n_cfg/%s/hv", testNisdUUID), testHV, "")
				ds.Write(fmt.Sprintf("n_cfg/%s/ts", testNisdUUID), "1000000000000", "")
				ds.Write(fmt.Sprintf("n_cfg/%s/as", testNisdUUID), "1000000000000", "")
				ds.Write(fmt.Sprintf("n_cfg/%s/p", testNisdUUID), testPDU, "")
				ds.Write(fmt.Sprintf("n_cfg/%s/r", testNisdUUID), testRack, "")
				ds.Write(fmt.Sprintf("n_cfg/%s/pt", testNisdUUID), testPT, "")
				ds.Write(fmt.Sprintf("n_cfg/%s/nic", testNisdUUID), "0", "")
			},

			setupHR: func() {
				t.Log("Initializing HR")

				HR.Init()

				nisd := &ctlplfl.Nisd{
					ID:            testNisdUUID,
					AvailableSize: 1073741824,
					FailureDomain: []string{testPDU, testRack, testHV, testDev, testPT},
				}

				HR.AddNisd(nisd)

				t.Log("Added NISD to HR:", nisd.ID)
			},

			vdevID:      testVdevUUID,
			expectError: false,

			verify: func(t *testing.T, ds storageiface.DataStore) {

				t.Log("Starting verification")

				_, err := ds.Read(fmt.Sprintf("v/%s/cfg/size", testVdevUUID), "")

				if err == nil {
					t.Error("Vdev metadata should be deleted")
				}

				_, err = ds.Read(fmt.Sprintf("v/%s/c/0/R.0", testVdevUUID), "")

				if err == nil {
					t.Error("Chunk allocation should be deleted")
				}

				_, err = ds.Read(fmt.Sprintf("n/%s/%s", testNisdUUID, testVdevUUID), "")

				if err == nil {
					t.Error("NISD reverse mapping should be deleted")
				}

				res, err := ds.Read(fmt.Sprintf("n_cfg/%s/as", testNisdUUID), "")

				expectedAS := 1000000000000 + 8589934592

				if err != nil || string(res) != strconv.FormatInt(int64(expectedAS), 10) {
					t.Errorf("Expected NISD AS %d, got %s", expectedAS, string(res))
				}

				nisd, _ := HR.GetNisdByPDUID(testPDU, testNisdUUID)

				if nisd == nil || nisd.AvailableSize != int64(expectedAS) {
					t.Errorf("HR NISD AS not updated properly")
				}

				t.Log("Verification completed")
			},
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			t.Log("Running test case:", tc.name)

			ds := newTestDataStore()
			if tc.setupData != nil {
				tc.setupData(ds)
			}
			if tc.setupHR != nil {
				tc.setupHR()
			}

			cbArgs := &PumiceDBServer.PmdbCbArgs{
				Store:     ds,
				ReplySize: 4096,
			}

			token, _ := createTestToken(testAdminID, "admin", testSecret)
			cpReq := ctlplfl.CPReq{
				Token:   token,
				Payload: ctlplfl.DeleteVdevReq{ID: tc.vdevID},
			}

			result, err := APDeleteVdev(cpReq, cbArgs)

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var cpResp ctlplfl.CPResp

			if decErr := pmCmn.Decoder(pmCmn.GOB, result.([]byte), &cpResp); decErr != nil {
				t.Fatalf("Failed to decode response: %v", decErr)
			}

			t.Log("Decoded CPResp errorMsg:", cpResp.Err())

			if cpResp.Error != nil {
				t.Errorf("Expected success response, got error: %s", cpResp.Err())
			}

			if tc.verify != nil {
				t.Log("Running verify")
				tc.verify(t, ds)
			}

			t.Log("Test case finished:", tc.name)
		})
	}
}

// seedHierarchyStore writes the minimal set of keys for a single
// PDU -> Rack -> HV -> Device (with one partition) into ds.
// Key formats follow the element-key constants used by the parsers:
//

func seedHierarchyStore(ds storageiface.DataStore, pduID, rackID, hvID, devID, ptID string) {
	// PDU
	ds.Write(fmt.Sprintf("p/%s/nm", pduID), "test-pdu", "")
	ds.Write(fmt.Sprintf("p/%s/l", pduID), "DC-Test", "")

	// Rack  ("p" stores the PDU UUID it belongs to)
	ds.Write(fmt.Sprintf("r/%s/nm", rackID), "test-rack", "")
	ds.Write(fmt.Sprintf("r/%s/p", rackID), pduID, "")

	// Hypervisor  ("r" stores the rack UUID)
	ds.Write(fmt.Sprintf("hv/%s/nm", hvID), "test-hv", "")
	ds.Write(fmt.Sprintf("hv/%s/r", hvID), rackID, "")
	ds.Write(fmt.Sprintf("hv/%s/ip/192.168.1.10", hvID), "9000", "")

	// Device  ("hv" stores the hypervisor UUID)
	ds.Write(fmt.Sprintf("d_cfg/%s/hv", devID), hvID, "")
	ds.Write(fmt.Sprintf("d_cfg/%s/sn", devID), "SN-TEST-001", "")

	// Partition under the device
	ds.Write(fmt.Sprintf("d_cfg/%s/pt/%s/dev_id", devID, ptID), devID, "")
	ds.Write(fmt.Sprintf("d_cfg/%s/pt/%s/pt_path", devID, ptID), "/dev/nvme0n1p1", "")
}

func TestReadHierarchy(t *testing.T) {
	// ReadHierarchy calls validateAndAuthorizeRBAC for authz.ReadHierarchy.
	authorizer = authz.NewAuthorizerWithConfig(authz.Config{
		authz.ReadHierarchy: authz.FunctionPolicy{
			RBAC: []string{"admin", "user"},
		},
	})
	defer func() { authorizer = nil }()

	const (
		hierPDU  = "11111111-1111-1111-1111-111111111111"
		hierRack = "22222222-2222-2222-2222-222222222222"
		hierHV   = "33333333-3333-3333-3333-333333333333"
		hierDev  = "nvme-hier-001"
		hierPT   = "nvme-hier-001-p1"
	)

	testCases := []struct {
		name            string
		setupData       func(storageiface.DataStore)
		setupToken      func() string
		page            *ctlplfl.Pagination
		expectError     bool
		errorContains   string
		expectedErrCode ctlplfl.CPErrCode
		validate        func(t *testing.T, pdus []ctlplfl.PDU)
	}{
		{
			// Missing token must be rejected before touching the store.
			name:      "AuthFailure_MissingToken",
			setupData: func(_ storageiface.DataStore) {},
			setupToken: func() string {
				return ""
			},
			expectError:     true,
			errorContains:   "user token is required",
			expectedErrCode: ctlplfl.ErrAuth,
		},
		{
			// Role not listed in the policy must be denied.
			name:      "AuthFailure_UnauthorizedRole",
			setupData: func(_ storageiface.DataStore) {},
			setupToken: func() string {
				tok, _ := createTestToken(testUserID1, "viewer", testSecret)
				return tok
			},
			expectError:     true,
			errorContains:   "authorization failed",
			expectedErrCode: ctlplfl.ErrAuth,
		},
		{
			// Valid token, empty store -> response is an empty PDU slice.
			name:      "EmptyStore_ReturnsEmptyList",
			setupData: func(_ storageiface.DataStore) {},
			setupToken: func() string {
				tok, _ := createTestToken(testUserID1, "user", testSecret)
				return tok
			},
			validate: func(t *testing.T, pdus []ctlplfl.PDU) {
				if len(pdus) != 0 {
					t.Errorf("expected 0 PDUs for empty store, got %d", len(pdus))
				}
			},
		},
		{
			// Full hierarchy: verifies PDU->Rack->HV->Device->Partition linking
			// and that NISDs are absent (excluded by ReadHierarchy).
			name: "FullHierarchy_LinkedCorrectly",
			setupData: func(ds storageiface.DataStore) {
				seedHierarchyStore(ds, hierPDU, hierRack, hierHV, hierDev, hierPT)
			},
			setupToken: func() string {
				tok, _ := createTestToken(testAdminID, "admin", testSecret)
				return tok
			},
			validate: func(t *testing.T, pdus []ctlplfl.PDU) {
				var found *ctlplfl.PDU
				for i := range pdus {
					if pdus[i].ID == hierPDU {
						found = &pdus[i]
						break
					}
				}
				if found == nil {
					t.Fatalf("PDU %s not found in hierarchy response", hierPDU)
				}

				// Rack linked under PDU
				if len(found.Racks) != 1 || found.Racks[0].ID != hierRack {
					t.Errorf("expected rack %s under PDU, got %+v", hierRack, found.Racks)
				}
				rack := found.Racks[0]
				if rack.PDUID != hierPDU {
					t.Errorf("rack.PDUID: want %s, got %s", hierPDU, rack.PDUID)
				}

				// Hypervisor linked under Rack
				if len(rack.Hypervisors) != 1 || rack.Hypervisors[0].ID != hierHV {
					t.Errorf("expected HV %s under Rack, got %+v", hierHV, rack.Hypervisors)
				}
				hv := rack.Hypervisors[0]
				if hv.RackID != hierRack {
					t.Errorf("hv.RackID: want %s, got %s", hierRack, hv.RackID)
				}

				// Device linked under Hypervisor
				if len(hv.Dev) != 1 || hv.Dev[0].ID != hierDev {
					t.Errorf("expected device %s under HV, got %+v", hierDev, hv.Dev)
				}
				dev := hv.Dev[0]
				if dev.HypervisorID != hierHV {
					t.Errorf("dev.HypervisorID: want %s, got %s", hierHV, dev.HypervisorID)
				}

				// Partition present under Device
				if len(dev.Partitions) != 1 || dev.Partitions[0].PartitionID != hierPT {
					t.Errorf("expected partition %s under Device, got %+v", hierPT, dev.Partitions)
				}
			},
		},
		{
			// Pagination: page-size=1 with one PDU seeded.
			// Result must contain exactly 1 PDU.
			name: "Pagination_PageSizeOne",
			setupData: func(ds storageiface.DataStore) {
				seedHierarchyStore(ds, hierPDU, hierRack, hierHV, hierDev, hierPT)
			},
			setupToken: func() string {
				tok, _ := createTestToken(testAdminID, "admin", testSecret)
				return tok
			},
			page: &ctlplfl.Pagination{SeqNo: 0, PageSize: 1},
			validate: func(t *testing.T, pdus []ctlplfl.PDU) {
				if len(pdus) != 1 {
					t.Errorf("expected 1 PDU on first page (size=1), got %d", len(pdus))
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ds := memstore.NewMemStore()
			colmfamily = ""
			if tc.setupData != nil {
				tc.setupData(ds)
			}

			cbArgs := &PumiceDBServer.PmdbCbArgs{
				Store:     ds,
				ReplySize: 4 * 1024 * 1024,
			}
			cpReq := ctlplfl.CPReq{
				Token: tc.setupToken(),
				Page:  tc.page,
			}

			result, err := ReadHierarchy(cbArgs, cpReq)

			if tc.expectError {
				if result == nil {
					t.Fatal("expected encoded error response, got nil")
				}
				var cpResp ctlplfl.CPResp
				if decErr := pmCmn.Decoder(pmCmn.GOB, result.([]byte), &cpResp); decErr != nil {
					t.Fatalf("failed to decode CPResp: %v", decErr)
				}
				if cpResp.Error == nil {
					t.Fatalf("expected CPResp.Error, got success (payload %T)", cpResp.Payload)
				}
				if tc.errorContains != "" && !strings.Contains(cpResp.Error.Message, tc.errorContains) {
					t.Errorf("error message: want %q, got %q", tc.errorContains, cpResp.Error.Message)
				}
				if tc.expectedErrCode != "" && cpResp.Error.Code != tc.expectedErrCode {
					t.Errorf("error code: want %q, got %q", tc.expectedErrCode, cpResp.Error.Code)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected Go error: %v", err)
			}
			if result == nil {
				t.Fatal("expected non-nil result")
			}

			var cpResp ctlplfl.CPResp
			if decErr := pmCmn.Decoder(pmCmn.GOB, result.([]byte), &cpResp); decErr != nil {
				t.Fatalf("failed to decode CPResp: %v", decErr)
			}
			if cpResp.Error != nil {
				t.Fatalf("expected success, got CPResp error: %s", cpResp.Err())
			}

			pdus, ok := cpResp.Payload.([]ctlplfl.PDU)
			if !ok {
				t.Fatalf("expected payload []ctlplfl.PDU, got %T", cpResp.Payload)
			}
			if tc.validate != nil {
				tc.validate(t, pdus)
			}
		})
	}
}

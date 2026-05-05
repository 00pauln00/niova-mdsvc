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
	log.InitXlog("./test.log", &logLevel)

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

			ds := memstore.NewMemStore()
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

// ─── Pagination Tests ───────────────────────────────────────────────────────

// seedNisds writes n NISD configs to the datastore for pagination testing.
func seedNisds(t *testing.T, ds storageiface.DataStore, n int) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("nisd-%04d-0000-0000-0000-000000000000", i)
		ids[i] = id
		ds.Write(fmt.Sprintf("n_cfg/%s/d", id), fmt.Sprintf("dev-%d", i), "")
		ds.Write(fmt.Sprintf("n_cfg/%s/pp", id), strconv.Itoa(8000+i), "")
		ds.Write(fmt.Sprintf("n_cfg/%s/ts", id), "1000000000", "")
		ds.Write(fmt.Sprintf("n_cfg/%s/as", id), "500000000", "")
	}
	return ids
}

// seedRacks writes n Rack configs to the datastore.
func seedRacks(t *testing.T, ds storageiface.DataStore, n int) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("rack-%04d-0000-0000-0000-000000000000", i)
		ids[i] = id
		ds.Write(fmt.Sprintf("r/%s/nm", id), fmt.Sprintf("Rack-%d", i), "")
		ds.Write(fmt.Sprintf("r/%s/l", id), fmt.Sprintf("location-%d", i), "")
	}
	return ids
}

// seedPDUs writes n PDU configs to the datastore.
func seedPDUs(t *testing.T, ds storageiface.DataStore, n int) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("pdu-%04d-0000-0000-0000-000000000000", i)
		ids[i] = id
		ds.Write(fmt.Sprintf("p/%s/nm", id), fmt.Sprintf("PDU-%d", i), "")
		ds.Write(fmt.Sprintf("p/%s/l", id), fmt.Sprintf("location-%d", i), "")
	}
	return ids
}

// decodePaginatedResponse decodes a paginated CPResp and returns payload and pagination info.
func decodePaginatedResponse(t *testing.T, result interface{}, target any) *ctlplfl.CPResp {
	t.Helper()
	cpResp := &ctlplfl.CPResp{Payload: target}
	if err := pmCmn.Decoder(pmCmn.GOB, result.([]byte), cpResp); err != nil {
		t.Fatalf("failed to decode CPResp: %v", err)
	}
	if cpResp.Error != nil {
		t.Fatalf("unexpected error in response: %s", cpResp.Error.Message)
	}
	return cpResp
}

func TestReadAllNisdConfigs_Paginated(t *testing.T) {
	authorizer = nil
	defer func() { authorizer = nil }()
	SetAuthEnabled(false)
	defer SetAuthEnabled(true)

	ds := memstore.NewMemStore()
	colmfamily = ""
	seedNisds(t, ds, 5)

	cbArgs := &PumiceDBServer.PmdbCbArgs{
		Store:     ds,
		ReplySize: 4096,
	}

	token, _ := createTestToken(testAdminID, "admin", testSecret)
	cpReq := ctlplfl.CPReq{
		Token:   token,
		Payload: ctlplfl.GetReq{GetAll: true},
		Page:    &ctlplfl.Pagination{},
	}

	result, err := ReadAllNisdConfigs(cbArgs, cpReq)
	if err != nil {
		t.Fatalf("ReadAllNisdConfigs failed: %v", err)
	}

	var nisds []ctlplfl.Nisd
	cpResp := decodePaginatedResponse(t, result, &nisds)

	if len(nisds) != 5 {
		t.Errorf("expected 5 nisds, got %d", len(nisds))
	}

	// With 5 small NISDs, all should fit in one page (under 4MB)
	if cpResp.Page != nil && cpResp.Page.Token != "" {
		t.Errorf("expected no more pages, but got nextKey=%q", cpResp.Page.Token)
	}

	t.Logf("ReadAllNisdConfigs returned %d nisds in single page", len(nisds))
}

func TestReadRackCfg_Paginated(t *testing.T) {
	authorizer = nil
	defer func() { authorizer = nil }()
	SetAuthEnabled(false)
	defer SetAuthEnabled(true)

	ds := memstore.NewMemStore()
	colmfamily = ""
	expectedIDs := seedRacks(t, ds, 3)

	cbArgs := &PumiceDBServer.PmdbCbArgs{
		Store:     ds,
		ReplySize: 4096,
	}

	token, _ := createTestToken(testAdminID, "admin", testSecret)

	t.Run("GetAll_SinglePage", func(t *testing.T) {
		cpReq := ctlplfl.CPReq{
			Token:   token,
			Payload: ctlplfl.GetReq{GetAll: true},
			Page:    &ctlplfl.Pagination{},
		}

		result, err := ReadRackCfg(cbArgs, cpReq)
		if err != nil {
			t.Fatalf("ReadRackCfg failed: %v", err)
		}

		var racks []ctlplfl.Rack
		cpResp := decodePaginatedResponse(t, result, &racks)

		if len(racks) != 3 {
			t.Errorf("expected 3 racks, got %d", len(racks))
		}

		// Verify all expected IDs are present
		gotIDs := make(map[string]bool)
		for _, r := range racks {
			gotIDs[r.ID] = true
		}
		for _, id := range expectedIDs {
			if !gotIDs[id] {
				t.Errorf("missing expected rack ID: %s", id)
			}
		}

		if cpResp.Page != nil && cpResp.Page.Token != "" {
			t.Errorf("expected no more pages, but got nextKey=%q", cpResp.Page.Token)
		}

		t.Logf("ReadRackCfg GetAll returned %d racks in single page", len(racks))
	})

	t.Run("GetSingle", func(t *testing.T) {
		targetID := expectedIDs[0]
		cpReq := ctlplfl.CPReq{
			Token:   token,
			Payload: ctlplfl.GetReq{ID: targetID},
			Page:    &ctlplfl.Pagination{},
		}

		result, err := ReadRackCfg(cbArgs, cpReq)
		if err != nil {
			t.Fatalf("ReadRackCfg failed: %v", err)
		}

		var racks []ctlplfl.Rack
		decodePaginatedResponse(t, result, &racks)

		if len(racks) != 1 {
			t.Fatalf("expected 1 rack, got %d", len(racks))
		}
		if racks[0].ID != targetID {
			t.Errorf("expected rack ID %q, got %q", targetID, racks[0].ID)
		}

		t.Logf("ReadRackCfg single return rack ID=%s", racks[0].ID)
	})
}

func TestReadPDUCfg_Paginated(t *testing.T) {
	authorizer = nil
	defer func() { authorizer = nil }()
	SetAuthEnabled(false)
	defer SetAuthEnabled(true)

	ds := memstore.NewMemStore()
	colmfamily = ""
	expectedIDs := seedPDUs(t, ds, 4)

	cbArgs := &PumiceDBServer.PmdbCbArgs{
		Store:     ds,
		ReplySize: 4096,
	}

	token, _ := createTestToken(testAdminID, "admin", testSecret)

	t.Run("GetAll_SinglePage", func(t *testing.T) {
		cpReq := ctlplfl.CPReq{
			Token:   token,
			Payload: ctlplfl.GetReq{GetAll: true},
			Page:    &ctlplfl.Pagination{},
		}

		result, err := ReadPDUCfg(cbArgs, cpReq)
		if err != nil {
			t.Fatalf("ReadPDUCfg failed: %v", err)
		}

		var pdus []ctlplfl.PDU
		cpResp := decodePaginatedResponse(t, result, &pdus)

		if len(pdus) != 4 {
			t.Errorf("expected 4 PDUs, got %d", len(pdus))
		}

		gotIDs := make(map[string]bool)
		for _, p := range pdus {
			gotIDs[p.ID] = true
		}
		for _, id := range expectedIDs {
			if !gotIDs[id] {
				t.Errorf("missing expected PDU ID: %s", id)
			}
		}

		if cpResp.Page != nil && cpResp.Page.Token != "" {
			t.Errorf("expected no more pages, but got nextKey=%q", cpResp.Page.Token)
		}

		t.Logf("ReadPDUCfg GetAll returned %d PDUs in single page", len(pdus))
	})

	t.Run("GetSingle", func(t *testing.T) {
		targetID := expectedIDs[1]
		cpReq := ctlplfl.CPReq{
			Token:   token,
			Payload: ctlplfl.GetReq{ID: targetID},
			Page:    &ctlplfl.Pagination{},
		}

		result, err := ReadPDUCfg(cbArgs, cpReq)
		if err != nil {
			t.Fatalf("ReadPDUCfg failed: %v", err)
		}

		var pdus []ctlplfl.PDU
		decodePaginatedResponse(t, result, &pdus)

		if len(pdus) != 1 {
			t.Fatalf("expected 1 PDU, got %d", len(pdus))
		}
		if pdus[0].ID != targetID {
			t.Errorf("expected PDU ID %q, got %q", targetID, pdus[0].ID)
		}

		t.Logf("ReadPDUCfg single returned PDU ID=%s", pdus[0].ID)
	})
}

// TestPaginatedResponse_NoDuplicates verifies that paginated multi-page traversal
// across ReadAllNisdConfigs produces no duplicate IDs.
func TestPaginatedResponse_NoDuplicates(t *testing.T) {
	authorizer = nil
	defer func() { authorizer = nil }()
	SetAuthEnabled(false)
	defer SetAuthEnabled(true)

	ds := memstore.NewMemStore()
	colmfamily = ""
	expectedIDs := seedNisds(t, ds, 10)

	cbArgs := &PumiceDBServer.PmdbCbArgs{
		Store:     ds,
		ReplySize: 4096,
	}

	token, _ := createTestToken(testAdminID, "admin", testSecret)
	seen := make(map[string]int)
	page := &ctlplfl.Pagination{}

	for pageNum := 0; pageNum < 100; pageNum++ { // cap at 100 to avoid infinite loops
		cpReq := ctlplfl.CPReq{
			Token:   token,
			Payload: ctlplfl.GetReq{GetAll: true},
			Page:    page,
		}

		result, err := ReadAllNisdConfigs(cbArgs, cpReq)
		if err != nil {
			t.Fatalf("page %d: ReadAllNisdConfigs failed: %v", pageNum, err)
		}

		var nisds []ctlplfl.Nisd
		cpResp := decodePaginatedResponse(t, result, &nisds)

		for _, n := range nisds {
			seen[n.ID]++
			if seen[n.ID] > 1 {
				t.Errorf("duplicate NISD ID %q on page %d", n.ID, pageNum)
			}
		}

		if cpResp.Page == nil || cpResp.Page.Token == "" {
			break
		}
		page = cpResp.Page
	}

	if len(seen) != len(expectedIDs) {
		t.Errorf("expected %d unique NISDs, got %d", len(expectedIDs), len(seen))
	}

	t.Logf("traversed all pages, found %d unique NISDs with no duplicates", len(seen))
}

// TestReadChunksInfoPaginated verifies that ReadChunksInfoPaginated correctly reads
// all chunk-to-NISD mappings for a vdev, grouping replica entries per chunk.
func TestReadChunksInfoPaginated(t *testing.T) {
	authorizer = nil
	defer func() { authorizer = nil }()
	SetAuthEnabled(false)
	defer SetAuthEnabled(true)

	const (
		testVdev  = "019df469-da70-705d-ad48-0444cda4a67e"
		testNisd0 = "ed7914c3-2e96-4f3e-8e0d-000000000001"
		testNisd1 = "ed7914c3-2e96-4f3e-8e0d-000000000002"
		testNisd2 = "f438cfd7-03c8-4a40-8a12-000000000001"
		testNisd3 = "f438cfd7-03c8-4a40-8a12-000000000002"
		testNisd4 = "a1b2c3d4-0000-0000-0000-000000000001"
		testNisd5 = "a1b2c3d4-0000-0000-0000-000000000002"
	)

	ds := memstore.NewMemStore()
	colmfamily = ""

	// Seed: 3 chunks, each with 2 replicas
	// chunk 0 → replica R.0 → testNisd0, R.1 → testNisd1
	// chunk 1 → replica R.0 → testNisd2, R.1 → testNisd3
	// chunk 2 → replica R.0 → testNisd4, R.1 → testNisd5
	entries := []struct{ key, val string }{
		{fmt.Sprintf("v/%s/c/0/R.0", testVdev), testNisd0},
		{fmt.Sprintf("v/%s/c/0/R.1", testVdev), testNisd1},
		{fmt.Sprintf("v/%s/c/1/R.0", testVdev), testNisd2},
		{fmt.Sprintf("v/%s/c/1/R.1", testVdev), testNisd3},
		{fmt.Sprintf("v/%s/c/2/R.0", testVdev), testNisd4},
		{fmt.Sprintf("v/%s/c/2/R.1", testVdev), testNisd5},
	}
	for _, e := range entries {
		if err := ds.Write(e.key, e.val, ""); err != nil {
			t.Fatalf("failed to seed datastore: %v", err)
		}
	}

	cbArgs := &PumiceDBServer.PmdbCbArgs{
		Store:     ds,
		ReplySize: 4 * 1024 * 1024,
	}
	token, _ := createTestToken(testAdminID, "admin", testSecret)
	cpReq := ctlplfl.CPReq{
		Token:   token,
		Payload: ctlplfl.GetReq{ID: testVdev},
		Page:    &ctlplfl.Pagination{},
	}

	result, err := ReadChunksInfoPaginated(cbArgs, cpReq)
	if err != nil {
		t.Fatalf("ReadChunksInfoPaginated returned error: %v", err)
	}

	var chunks []ctlplfl.ChunkInfo
	cpResp := decodePaginatedResponse(t, result, &chunks)

	// Verify: 3 chunks returned in a single page
	if len(chunks) != 3 {
		t.Errorf("expected 3 chunks, got %d", len(chunks))
	}

	// Verify no more pages
	if cpResp.Page != nil && cpResp.Page.Token != "" {
		t.Errorf("expected no more pages, but got nextKey=%q", cpResp.Page.Token)
	}

	// Build a map of chunkIdx → NisdUUIDs for easier assertions
	chunkMap := make(map[int][]string)
	for _, ci := range chunks {
		chunkMap[ci.ChunkIdx] = ci.NisdUUIDs
		if ci.NumReplicas != 2 {
			t.Errorf("chunk %d: expected NumReplicas=2, got %d", ci.ChunkIdx, ci.NumReplicas)
		}
	}

	expected := map[int][]string{
		0: {testNisd0, testNisd1},
		1: {testNisd2, testNisd3},
		2: {testNisd4, testNisd5},
	}
	for idx, wantNisds := range expected {
		gotNisds, ok := chunkMap[idx]
		if !ok {
			t.Errorf("chunk %d missing from response", idx)
			continue
		}
		gotSet := make(map[string]struct{}, len(gotNisds))
		for _, n := range gotNisds {
			gotSet[n] = struct{}{}
		}
		for _, n := range wantNisds {
			if _, found := gotSet[n]; !found {
				t.Errorf("chunk %d: expected NISD %s not found in %v", idx, n, gotNisds)
			}
		}
	}

	t.Logf("TestReadChunksInfoPaginated passed: %d chunks returned", len(chunks))
}

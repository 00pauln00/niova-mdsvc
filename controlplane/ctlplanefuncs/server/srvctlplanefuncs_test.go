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

const (
	testVdevUUID = "28061cd0-1e01-11f1-a069-032bff036f03"
	testNisdUUID = "59ee0460-1e01-11f1-9566-83949aa998ea"

	testPDU  = "acdef556-1ea3-11f1-848b-9f6e716afc46"
	testRack = "b1b89a50-1ea3-11f1-b397-d76191bdb3d2"
	testHV   = "b726b99a-1ea3-11f1-95da-436ff27bf77e"
	testDev  = "nvme-001"
	testPT   = "nvme-001-01"

	testNisdAvailableSize = 1000000000
	testVdevSize          = 1073741824
)

// TestMain initializes the test environment
func TestMain(m *testing.M) {
	// Initialize xlog to prevent nil pointer errors
	logLevel := "INFO"
	log.InitXlog("/tmp/test.log", &logLevel)

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
	testSecret  = []byte(ctlplfl.CP_SECRET)
	wrongSecret = []byte("wrong-secret")
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

// Helper function to verify ownership key exists in datastore
func verifyOwnershipKey(ds storageiface.DataStore, userID, vdevID string) bool {
	ownershipKey := fmt.Sprintf("/u/%s/v/%s", userID, vdevID)
	result, err := ds.Read(ownershipKey, "")
	if err != nil {
		return false
	}
	return string(result) == "1"
}

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
	authorizer = &authz.Authorizer{
		Config: authz.Config{
			"WPCreateVdev": authz.FunctionPolicy{
				RBAC: []string{"admin", "user"},
			},
		},
	}
	defer func() { authorizer = nil }()

	testCases := []struct {
		name           string
		setupToken     func() string
		vdevSize       int64
		numChunks      int
		numReplica     int
		expectError    bool
		errorContains  string
		checkOwnership bool
		expectedUserID string
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
			vdevSize:      1073741824,
			numChunks:     4,
			numReplica:    3,
			expectError:   true,
			errorContains: "user token is required",
		},
		{
			name: "MissingUserIDClaim",
			setupToken: func() string {
				tc := &auth.Token{Secret: testSecret, TTL: time.Hour}
				claims := map[string]any{"role": "user"} // Missing userID
				token, _ := tc.CreateToken(claims)
				return token
			},
			vdevSize:      1073741824,
			numChunks:     4,
			numReplica:    3,
			expectError:   true,
			errorContains: "missing userID",
		},
		{
			name: "MissingRoleClaim",
			setupToken: func() string {
				tc := &auth.Token{Secret: testSecret, TTL: time.Hour}
				claims := map[string]any{"userID": testUserID1} // Missing role
				token, _ := tc.CreateToken(claims)
				return token
			},
			vdevSize:      1073741824,
			numChunks:     4,
			numReplica:    3,
			expectError:   true,
			errorContains: "missing role",
		},
		{
			name: "UnauthorizedRole",
			setupToken: func() string {
				token, _ := createTestToken(testUserID1, "viewer", testSecret)
				return token
			},
			vdevSize:      1073741824,
			numChunks:     4,
			numReplica:    3,
			expectError:   true,
			errorContains: "authorization failed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test vdev
			vdev := ctlplfl.Vdev{
				Cfg: ctlplfl.VdevCfg{
					Size:       tc.vdevSize,
					NumChunks:  uint32(tc.numChunks),
					NumReplica: uint8(tc.numReplica),
				},
				UserToken: tc.setupToken(),
			}

			// Call WPCreateVdev
			result, err := WPCreateVdev(vdev)

			// Check error expectations
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error containing '%s', but got no error", tc.errorContains)
					return
				}
				if tc.errorContains != "" && !contains(err.Error(), tc.errorContains) {
					t.Errorf("Expected error containing '%s', but got '%s'", tc.errorContains, err.Error())
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
					if contains(key, expectedOwnershipKey) && string(chg.Value) == "1" {
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
	vdev := ctlplfl.Vdev{
		Cfg: ctlplfl.VdevCfg{
			Size:       1073741824,
			NumChunks:  4,
			NumReplica: 3,
		},
		UserToken: token,
	}

	// Should succeed even without authorizer (graceful degradation)
	result, err := WPCreateVdev(vdev)
	if err != nil {
		t.Errorf("Expected success with nil authorizer, but got error: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestReadVdevInfo(t *testing.T) {
	// Initialize test authorizer
	authorizer = &authz.Authorizer{
		Config: authz.Config{
			"ReadVdevInfo": authz.FunctionPolicy{
				RBAC: []string{"admin", "user"},
				ABAC: []authz.ABACRule{
					{Argument: "vdev", Prefix: "v/"},
				},
			},
		},
	}
	defer func() { authorizer = nil }()

	testCases := []struct {
		name          string
		setupData     func(storageiface.DataStore)
		setupToken    func() string
		vdevID        string
		expectError   bool
		errorContains string
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
			vdevID:        testVdevID,
			expectError:   true,
			errorContains: "user token is required",
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
			vdevID:        testVdevID,
			expectError:   true,
			errorContains: "authorization failed",
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
			vdevID:        testVdevID,
			expectError:   true,
			errorContains: "authorization failed",
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
			vdevID:        "",
			expectError:   true,
			errorContains: "Recieved empty ID",
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

			// Create GetReq
			req := ctlplfl.GetReq{
				ID:        tc.vdevID,
				GetAll:    false,
				UserToken: tc.setupToken(),
			}

			// Call ReadVdevInfo
			result, err := ReadVdevInfo(cbArgs, req)

			// Check error expectations
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error containing '%s', but got no error", tc.errorContains)
					return
				}
				if tc.errorContains != "" && !contains(err.Error(), tc.errorContains) {
					t.Errorf("Expected error containing '%s', but got '%s'", tc.errorContains, err.Error())
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
			}
		})
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestWPDeleteVdev(t *testing.T) {
	// Initialize authorizer for "WPDeleteVdev"
	authorizer = &authz.Authorizer{
		Config: authz.Config{
			"WPDeleteVdev": authz.FunctionPolicy{
				RBAC: []string{"admin"},
			},
		},
	}
	defer func() { authorizer = nil }()

	testCases := []struct {
		name          string
		setupToken    func() string
		vdevID        string
		expectError   bool
		errorContains string
	}{
		{
			name: "SuccessfulValidation",
			setupToken: func() string {
				token, _ := createTestToken(testAdminID, "admin", testSecret)
				return token
			},
			vdevID:      "28061cd0-1e01-11f1-a069-032bff036f03",
			expectError: false,
		},
		{
			name:   "InvalidRequest_EmptyID",
			vdevID: "",
			setupToken: func() string {
				token, _ := createTestToken(testAdminID, "admin", testSecret)
				return token
			},
			expectError:   true,
			errorContains: "invalid",
		},
		{
			name: "MissingToken",
			setupToken: func() string {
				return ""
			},
			vdevID:        "28061cd0-1e01-11f1-a069-032bff036f03",
			expectError:   true,
			errorContains: "user token is required",
		},
		{
			name: "UnauthorizedRole_Viewer",
			setupToken: func() string {
				token, _ := createTestToken(testUserID1, "viewer", testSecret)
				return token
			},
			vdevID:        "28061cd0-1e01-11f1-a069-032bff036f03",
			expectError:   true,
			errorContains: "authorization failed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := ctlplfl.DeleteVdevReq{
				ID:        tc.vdevID,
				UserToken: tc.setupToken(),
			}

			result, err := WPDeleteVdev(req)

			if tc.expectError {
				if err != nil {
					if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
						t.Errorf("Expected error containing %q, got %q", tc.errorContains, err.Error())
					}
					return
				}
				// Check encoded response error
				if result == nil {
					t.Fatal("Expected non-nil result for encoded error response")
				}
				var resp ctlplfl.ResponseXML
				if err := pmCmn.Decoder(pmCmn.GOB, result.([]byte), &resp); err != nil {
					t.Fatalf("Failed to decode response: %v", err)
				}
				if !strings.Contains(resp.Error, tc.errorContains) {
					t.Errorf("Expected resp.Error to contain %q, got %q", tc.errorContains, resp.Error)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var intrm funclib.FuncIntrm
			if err := pmCmn.Decoder(pmCmn.GOB, result.([]byte), &intrm); err != nil {
				t.Fatalf("Failed to decode intermediate result: %v", err)
			}

			var decodedReq ctlplfl.DeleteVdevReq
			if err := pmCmn.Decoder(pmCmn.GOB, intrm.Response, &decodedReq); err != nil {
				t.Fatalf("Failed to decode embedded request: %v", err)
			}

			if decodedReq.ID != tc.vdevID {
				t.Errorf("Expected VdevID %q, got %q", tc.vdevID, decodedReq.ID)
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

				nisd, _ := HR.GetNisdByID(testPDU, testNisdUUID)

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

			req := ctlplfl.DeleteVdevReq{ID: tc.vdevID}

			result, err := APDeleteVdev(req, cbArgs)

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var resp ctlplfl.ResponseXML

			if err := pmCmn.Decoder(pmCmn.GOB, result.([]byte), &resp); err != nil {
				t.Fatalf("Failed to decode response: %v", err)
			}

			t.Log("Decoded response:", resp)

			if !resp.Success {
				t.Errorf("Expected success, got Error: %s", resp.Error)
			}

			if tc.verify != nil {
				t.Log("Running verify")
				tc.verify(t, ds)
			}

			t.Log("Test case finished:", tc.name)
		})
	}
}

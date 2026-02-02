package authorizer

import (
	"os"
	"testing"

	"github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/memstore"
)

func TestLoadConfig(t *testing.T) {
	// Create a dummy config file
	content := []byte(`
myFunc:
  RBAC:
    - admin
    - editor
  ABAC:
    - argument: "namespace"
      prefix: "ns/"
    - argument: "owner"
      prefix: "obj-owner/"
`)
	tmpfile, err := os.CreateTemp("", "config_test.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // clean up

	if _, err := tmpfile.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	auth := &Authorizer{
		Config: make(Config),
	}
	err = auth.LoadConfig(tmpfile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if _, ok := auth.Config["myFunc"]; !ok {
		t.Errorf("Expected to find 'myFunc' in config, but didn't")
	}

	policy := auth.Config["myFunc"]
	if len(policy.RBAC) != 2 {
		t.Errorf("Expected 2 RBAC roles, got %d", len(policy.RBAC))
	}
	if len(policy.ABAC) != 2 {
		t.Errorf("Expected 2 ABAC rules, got %d", len(policy.ABAC))
	}
}

func TestCheckRBAC(t *testing.T) {
	auth := &Authorizer{
		Config: Config{
			"myFunc": FunctionPolicy{
				RBAC: []string{"admin", "editor"},
			},
		},
	}

	testCases := []struct {
		name      string
		funcName  string
		userRoles []string
		expected  bool
	}{
		{"AdminRole", "myFunc", []string{"admin"}, true},
		{"EditorRole", "myFunc", []string{"editor"}, true},
		{"OtherRole", "myFunc", []string{"viewer"}, false},
		{"MultipleRolesWithOneMatch", "myFunc", []string{"viewer", "admin"}, true},
		{"NoMatchingFunction", "otherFunc", []string{"admin"}, false},
		{"NoRoles", "myFunc", []string{}, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := auth.CheckRBAC(tc.funcName, tc.userRoles)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestCheckABAC(t *testing.T) {
	auth := &Authorizer{
		Config: Config{
			"myFunc": FunctionPolicy{
				ABAC: []ABACRule{
					{Argument: "namespace", Prefix: "ns/"},
					{Argument: "owner", Prefix: "obj-owner/"},
				},
			},
		},
	}

	ds := memstore.NewMemStore()
	ds.Write("/u/user1/ns/dev", "1", "")
	ds.Write("/u/user1/obj-owner/resource1", "1", "")

	testCases := []struct {
		name       string
		funcName   string
		userID     string
		attributes map[string]string
		expected   bool
	}{
		{"AllAttributesMatch", "myFunc", "user1", map[string]string{"namespace": "dev", "owner": "resource1"}, true},
		{"OneAttributeMismatch", "myFunc", "user1", map[string]string{"namespace": "prod", "owner": "resource1"}, false},
		{"MissingAttribute", "myFunc", "user1", map[string]string{"namespace": "dev"}, false},
		{"WrongUser", "myFunc", "user2", map[string]string{"namespace": "dev", "owner": "resource1"}, false},
		{"NoMatchingFunction", "otherFunc", "user1", map[string]string{"namespace": "dev", "owner": "resource1"}, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := auth.CheckABAC(tc.funcName, tc.userID, tc.attributes, ds, "")
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestAuthorize(t *testing.T) {
	auth := &Authorizer{
		Config: Config{
			"myFunc": FunctionPolicy{
				RBAC: []string{"admin"},
				ABAC: []ABACRule{
					{Argument: "namespace", Prefix: "ns/"},
				},
			},
		},
	}

	ds := memstore.NewMemStore()
	ds.Write("/u/user1/ns/dev", "1", "")

	testCases := []struct {
		name       string
		funcName   string
		userID     string
		userRoles  []string
		attributes map[string]string
		expected   bool
	}{
		{"SuccessfulAuth", "myFunc", "user1", []string{"admin"}, map[string]string{"namespace": "dev"}, true},
		{"RBACFails", "myFunc", "user1", []string{"viewer"}, map[string]string{"namespace": "dev"}, false},
		{"ABACFails", "myFunc", "user1", []string{"admin"}, map[string]string{"namespace": "prod"}, false},
		{"BothFail", "myFunc", "user1", []string{"viewer"}, map[string]string{"namespace": "prod"}, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := auth.Authorize(tc.funcName, tc.userID, tc.userRoles, tc.attributes, ds, "")
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

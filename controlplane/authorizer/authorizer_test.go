package authorizer

import (
	"testing"

	"github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/memstore"
	"github.com/stretchr/testify/assert"
)

func TestCheckRBAC(t *testing.T) {
	auth := NewAuthorizerWithConfig(Config{
		WPRackCfg: {
			RBAC: []string{"admin", "editor"},
		},
	})

	tests := []struct {
		name      string
		fn        FunctionName
		roles     []string
		wantAllow bool
	}{
		{"AdminAllowed", WPRackCfg, []string{"admin"}, true},
		{"EditorAllowed", WPRackCfg, []string{"editor"}, true},
		{"ViewerDenied", WPRackCfg, []string{"viewer"}, false},
		{"MultiRoleOneMatch", WPRackCfg, []string{"viewer", "admin"}, true},
		{"UnknownFunction", ReadRackCfg, []string{"admin"}, false},
		{"NoRoles", WPRackCfg, []string{}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := auth.CheckRBAC(tc.fn, tc.roles)
			if got != tc.wantAllow {
				t.Errorf("CheckRBAC(%q, %v) = %v, want %v", tc.fn, tc.roles, got, tc.wantAllow)
			}
		})
	}
}

func TestCheckABAC(t *testing.T) {
	auth := NewAuthorizerWithConfig(Config{
		ReadVdevInfo: {
			ABAC: []ABACRule{
				{Argument: "vdev", Prefix: "v/"},
			},
		},
	})

	ds := memstore.NewMemStore()
	ds.Write("/u/user1/v/vdev-42", "1", "")

	tests := []struct {
		name       string
		fn         FunctionName
		userID     string
		attributes map[string]string
		wantAllow  bool
	}{
		{"MatchingAttribute", ReadVdevInfo, "user1", map[string]string{"vdev": "vdev-42"}, true},
		{"WrongAttributeValue", ReadVdevInfo, "user1", map[string]string{"vdev": "vdev-99"}, false},
		{"MissingAttribute", ReadVdevInfo, "user1", map[string]string{}, false},
		{"WrongUser", ReadVdevInfo, "user2", map[string]string{"vdev": "vdev-42"}, false},
		{"UnknownFunction", ReadRackCfg, "user1", map[string]string{"vdev": "vdev-42"}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := auth.CheckABAC(tc.fn, tc.userID, tc.attributes, ds, "")
			if got != tc.wantAllow {
				t.Errorf("CheckABAC(%q, %q, %v) = %v, want %v",
					tc.fn, tc.userID, tc.attributes, got, tc.wantAllow)
			}
		})
	}
}

func TestAuthorize(t *testing.T) {
	auth := NewAuthorizerWithConfig(Config{
		ReadVdevInfo: {
			RBAC: []string{"admin", "user"},
			ABAC: []ABACRule{
				{Argument: "vdev", Prefix: "v/"},
			},
		},
		WPRackCfg: {
			RBAC: []string{"admin"},
		},
	})

	ds := memstore.NewMemStore()
	err := ds.Write("/u/user1/v/vdev-42", "1", "")
	assert.NoError(t, err, "expected correct write for namespace")

	tests := []struct {
		name       string
		fn         FunctionName
		userID     string
		roles      []string
		attributes map[string]string
		wantAllow  bool
	}{
		{"RBAC+ABAC_pass", ReadVdevInfo, "user1", []string{"user"}, map[string]string{"vdev": "vdev-42"}, true},
		{"RBAC_fail", ReadVdevInfo, "user1", []string{"viewer"}, map[string]string{"vdev": "vdev-42"}, false},
		{"ABAC_fail", ReadVdevInfo, "user1", []string{"user"}, map[string]string{"vdev": "vdev-99"}, false},
		{"both_fail", ReadVdevInfo, "user1", []string{"viewer"}, map[string]string{"vdev": "vdev-99"}, false},
		{"AdminOnlyFunc_pass", WPRackCfg, "user1", []string{"admin"}, nil, true},
		{"AdminOnlyFunc_denied", WPRackCfg, "user1", []string{"user"}, nil, false},
		{"UnknownFunction", Login, "user1", []string{"admin"}, nil, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := auth.Authorize(tc.fn, tc.userID, tc.roles, tc.attributes, ds, "")
			if got != tc.wantAllow {
				t.Errorf("Authorize(%q, %q, %v, %v) = %v, want %v",
					tc.fn, tc.userID, tc.roles, tc.attributes, got, tc.wantAllow)
			}
		})
	}
}

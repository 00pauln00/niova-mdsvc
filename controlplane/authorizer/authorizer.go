package authorizer

import (
	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
)

type Authorizer struct {
	config Config
}

type Config = map[FunctionName]FunctionPolicy

type FunctionPolicy struct {
	RBAC []string   `yaml:"RBAC"`
	ABAC []ABACRule `yaml:"ABAC"`
}

type ABACRule struct {
	Argument string `yaml:"argument"`
	Prefix   string `yaml:"prefix"`
}

type FunctionName string

const (
	WPCreateVdev                  FunctionName = "WPCreateVdev"
	WPNisdCfg                     FunctionName = "WPNisdCfg"
	ApplyNisd                     FunctionName = "ApplyNisd"
	WPDeviceInfo                  FunctionName = "WPDeviceInfo"
	WPCreatePartition             FunctionName = "WPCreatePartition"
	WPPDUCfg                      FunctionName = "WPPDUCfg"
	WPRackCfg                     FunctionName = "WPRackCfg"
	WPHyperVisorCfg               FunctionName = "WPHyperVisorCfg"
	WPNisdArgs                    FunctionName = "WPNisdArgs"
	ReadAllNisdConfigs            FunctionName = "ReadAllNisdConfigs"
	ReadNisdConfig                FunctionName = "ReadNisdConfig"
	RdDeviceInfo                  FunctionName = "RdDeviceInfo"
	ReadPartition                 FunctionName = "ReadPartition"
	ReadPDUCfg                    FunctionName = "ReadPDUCfg"
	ReadRackCfg                   FunctionName = "ReadRackCfg"
	ReadHyperVisorCfg             FunctionName = "ReadHyperVisorCfg"
	ReadVdevsInfoWithChunkMapping FunctionName = "ReadVdevsInfoWithChunkMapping"
	ReadVdevInfo                  FunctionName = "ReadVdevInfo"
	APDeleteVdev                  FunctionName = "APDeleteVdev"
	ReadAllVdevInfo               FunctionName = "ReadAllVdevInfo"
	ReadChunkNisd                 FunctionName = "ReadChunkNisd"
	RdNisdArgs                    FunctionName = "RdNisdArgs"
	PutUser                       FunctionName = "PutUser"
	GetUser                       FunctionName = "GetUser"
	CreateAdminUser               FunctionName = "CreateAdminUser"
	Login                         FunctionName = "Login"
)

// defaultPolicies defines the full RBAC/ABAC policy for every registered function.
var defaultPolicies = map[FunctionName]FunctionPolicy{
	WPCreateVdev: {
		RBAC: []string{"admin", "user"},
	},
	WPNisdCfg: {
		RBAC: []string{"admin"},
	},
	ApplyNisd: {
		RBAC: []string{"admin"},
	},
	WPDeviceInfo: {
		RBAC: []string{"admin"},
	},
	WPCreatePartition: {
		RBAC: []string{"admin"},
	},
	WPPDUCfg: {
		RBAC: []string{"admin"},
	},
	WPRackCfg: {
		RBAC: []string{"admin"},
	},
	WPHyperVisorCfg: {
		RBAC: []string{"admin"},
	},
	WPNisdArgs: {
		RBAC: []string{"admin"},
	},
	ReadAllNisdConfigs: {
		RBAC: []string{"admin"},
	},
	ReadNisdConfig: {
		RBAC: []string{"admin", "user"},
	},
	RdDeviceInfo: {
		RBAC: []string{"admin"},
	},
	ReadPartition: {
		RBAC: []string{"admin"},
	},
	ReadPDUCfg: {
		RBAC: []string{"admin"},
	},
	ReadRackCfg: {
		RBAC: []string{"admin"},
	},
	ReadHyperVisorCfg: {
		RBAC: []string{"admin"},
	},
	ReadVdevsInfoWithChunkMapping: {
		RBAC: []string{"user", "admin"},
		ABAC: []ABACRule{
			{Argument: "vdev", Prefix: "v/"},
		},
	},
	ReadVdevInfo: {
		RBAC: []string{"user", "admin"},
		ABAC: []ABACRule{
			{Argument: "vdev", Prefix: "v/"},
		},
	},
	APDeleteVdev: {
		RBAC: []string{"admin"},
	},
	ReadAllVdevInfo: {
		RBAC: []string{"admin"},
	},
	ReadChunkNisd: {
		RBAC: []string{"user", "admin"},
		ABAC: []ABACRule{
			{Argument: "vdev", Prefix: "v/"},
		},
	},
	RdNisdArgs: {
		RBAC: []string{"admin"},
	},
	PutUser: {
		RBAC: []string{"admin"},
	},
	GetUser: {
		RBAC: []string{"admin", "user"},
	},
	CreateAdminUser: {
		RBAC: []string{"admin"},
	},
	Login: {
		RBAC: []string{"admin", "user"},
	},
}

// NewAuthorizer construct default policy Authorizer
func NewAuthorizer() *Authorizer {
	return &Authorizer{
		config: defaultPolicies,
	}
}

// NewAuthorizerWithConfig constructs an Authorizer with a custom policy map.
// Intended for use in tests that need a specific, limited policy set.
func NewAuthorizerWithConfig(cfg Config) *Authorizer {
	return &Authorizer{
		config: cfg,
	}
}

// getPolicy looks up the policy for fn under the read-lock.
func (a *Authorizer) getPolicy(fn FunctionName) (FunctionPolicy, bool) {
	p, ok := a.config[fn]
	return p, ok
}

// RBAC check
func (a *Authorizer) CheckRBAC(fn FunctionName, userRoles []string) bool {
	policy, ok := a.getPolicy(fn)
	if !ok {
		return false
	}

	roleSet := make(map[string]struct{})
	for _, r := range userRoles {
		roleSet[r] = struct{}{}
	}

	for _, allowedRole := range policy.RBAC {
		if _, exists := roleSet[allowedRole]; exists {
			return true
		}
	}
	return false
}

func prefixQuery(prefix, userID, value string, ds storageiface.DataStore, colFamily string) bool {
	res, err := ds.Read("/u/"+userID+"/"+prefix+value, colFamily)
	if err != nil {
		return false
	}
	if res == nil {
		return false
	}
	if string(res) == "1" {
		return true
	}
	return false
}

// ABAC check
func (a *Authorizer) CheckABAC(fn FunctionName, userID string, attributes map[string]string, ds storageiface.DataStore, colFamily string) bool {
	policy, ok := a.getPolicy(fn)
	if !ok {
		return false
	}

	for _, rule := range policy.ABAC {
		value, exists := attributes[rule.Argument]
		if !exists || !prefixQuery(rule.Prefix, userID, value, ds, colFamily) {
			return false
		}
	}
	return true
}

// Combined authorization
func (a *Authorizer) Authorize(fn FunctionName, userID string, userRoles []string, attributes map[string]string, ds storageiface.DataStore, colfamily string) bool {
	return a.CheckRBAC(fn, userRoles) && a.CheckABAC(fn, userID, attributes, ds, colfamily)
}

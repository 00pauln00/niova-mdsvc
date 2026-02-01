package authorizer

import (
	"os"

	"github.com/00pauln00/niova-pumicedb/go/pkg/pumicestore"
	"gopkg.in/yaml.v3"
)

type Authorizer struct {
	Config Config
}

type Config map[string]FunctionPolicy

type FunctionPolicy struct {
	RBAC []string   `yaml:"RBAC"`
	ABAC []ABACRule `yaml:"ABAC"`
}

type ABACRule struct {
	Argument string `yaml:"argument"`
	Prefix   string `yaml:"prefix"`
}

func (a *Authorizer) LoadConfig(filePath string) error {
	// Implementation for loading configuration goes here
	// Its an YAML file with following format
	/*
		function_name:
		  RBAC:
		    - role1
		    - role2
		  ABAC:
		    - argument: "arg1"
		      prefix: "prefix1/"
		    - argument: "arg2"
		      prefix: "prefix2/"
	*/
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(data, a.Config)
	if err != nil {
		return err
	}

	return nil
}

// RBAC check
func (a *Authorizer) CheckRBAC(funcName string, userRoles []string) bool {
	policy, ok := a.Config[funcName]
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

func prefixQuery(prefix, userID, value string, ds pumicestore.DataStore, colFamily string) bool {
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
func (a *Authorizer) CheckABAC(funcName string, userID string, attributes map[string]string, ds pumicestore.DataStore, colFamily string) bool {
	policy, ok := a.Config[funcName]
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
func (a *Authorizer) Authorize(funcName string, userID string, userRoles []string, attributes map[string]string, ds pumicestore.DataStore, colfamily string) bool {
	return a.CheckRBAC(funcName, userRoles) && a.CheckABAC(funcName, userID, attributes, ds, colfamily)
}

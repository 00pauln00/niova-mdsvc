package authorizer

import (
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

type Authorizer struct {
	Config    Config
	ColFamily string
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

func (a *Authorizer) prefixQuery(prefix, userID, value string) bool {
	var p pumiceserver.PmdbCbArgs
	res, err := p.PmdbReadKV(a.ColFamily, prefix+value+"/u/"+userID)
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
func (a *Authorizer) CheckABAC(funcName string, userID string, attributes map[string]string) bool {
	policy, ok := a.Config[funcName]
	if !ok {
		return false
	}

	for _, rule := range policy.ABAC {
		value, exists := attributes[rule.Argument]
		if !exists || !a.prefixQuery(rule.Prefix, userID, value) {
			return false
		}
	}
	return true
}

// Combined authorization
func (a *Authorizer) Authorize(funcName string, userID string, userRoles []string, attributes map[string]string) bool {
	return a.CheckRBAC(funcName, userRoles) && a.CheckABAC(funcName, userID, attributes)
}

package authorizer

type Authorizer struct {
	Config Config
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

func abacMatch(prefix, userID, value string) bool {
	// User prefix to query the rocksdb
	// value should be atleast in one of the key's substring
	// obtained using rangeRead(prefix := prefix + userID)
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
		//TODO: Change the check!
		if !exists || !abacMatch(rule.Prefix, userID, value) {
			return false
		}
	}
	return true
}

// Combined authorization
func (a *Authorizer) Authorize(funcName string, userID string, userRoles []string, attributes map[string]string) bool {
	return a.CheckRBAC(funcName, userRoles) && a.CheckABAC(funcName, attributes)
}

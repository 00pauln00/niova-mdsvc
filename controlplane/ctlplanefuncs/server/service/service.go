package service

import (
	"fmt"

	authz "github.com/00pauln00/niova-mdsvc/controlplane/authorizer"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"
)

type BaseService struct {
	authorizer *authz.Authorizer
	hierarchy  *srvctlplanefuncs.Hierarchy
}

func NewBaseService(auth *authz.Authorizer, hr *srvctlplanefuncs.Hierarchy) *BaseService {
	return &BaseService{authorizer: auth, hierarchy: hr}
}

func (s *BaseService) ValidateAndAuthorizeRBAC(token string, operation authz.FunctionName) (*srvctlplanefuncs.TokenClaims, error) {
	tc, err := srvctlplanefuncs.ValidateToken(token)
	if err != nil {
		return nil, &srvctlplanefuncs.ErrAuth{Err: err}
	}
	if s.authorizer != nil {
		if !s.authorizer.CheckRBAC(operation, []string{tc.Role}) {
			return nil, &srvctlplanefuncs.ErrAuth{Err: fmt.Errorf("authorization failed: insufficient permissions")}
		}
	}
	return tc, nil
}

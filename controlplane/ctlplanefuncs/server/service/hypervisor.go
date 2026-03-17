package service

import (
	log "github.com/00pauln00/niova-lookout/pkg/xlog"

	authz "github.com/00pauln00/niova-mdsvc/controlplane/authorizer"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"
	"github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server/repository"

	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

type HypervisorService struct {
	*BaseService
	repo *repository.StorageRepository
}

func NewHypervisorService(repo *repository.StorageRepository, base *BaseService) *HypervisorService {
	return &HypervisorService{BaseService: base, repo: repo}
}

func (s *HypervisorService) WPHyperVisorCfg(token string, hypv ctlplfl.Hypervisor) (*funclib.FuncIntrm, error) {
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.WPHyperVisorCfg); err != nil {
		log.Errorf("WPHyperVisorCfg: auth failure: %v", err)
		return nil, err
	}
	resp := &ctlplfl.ResponseXML{
		Name:    hypv.ID,
		Success: true,
	}
	commitChgs := srvctlplanefuncs.PopulateEntities(&hypv, srvctlplanefuncs.HvPopulator{}, srvctlplanefuncs.HvKey)
	return &funclib.FuncIntrm{Changes: commitChgs, Response: resp}, nil
}

func (s *HypervisorService) ReadHyperVisorCfg(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error) {
	key := srvctlplanefuncs.HvKey
	if !req.GetAll {
		key = srvctlplanefuncs.GetConfKey(srvctlplanefuncs.HvKey, req.ID)
	}
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.ReadHyperVisorCfg); err != nil {
		log.Errorf("ReadHyperVisorCfg: auth failure: %v", err)
		return ctlplfl.AuthError(err)
	}

	readResult, err := s.repo.RangeReadPrefix(cbArgs.Store, key, cbArgs.ReplySize)
	if err != nil {
		log.Error("Range read failure ", err)
		return ctlplfl.FuncError(err)
	}

	hvList := srvctlplanefuncs.ParseEntities[ctlplfl.Hypervisor](readResult.ResultMap, srvctlplanefuncs.HvParser{})
	log.Debugf("ReadHyperVisorCfg: returning %d hypervisor config(s) for key %s", len(hvList), key)
	return ctlplfl.EncodeResponse(hvList)

}

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

type RackService struct {
	*BaseService
	repo *repository.StorageRepository
}

func NewRackService(repo *repository.StorageRepository, base *BaseService) *RackService {
	return &RackService{
		BaseService: base,
		repo:        repo,
	}
}

func (s *RackService) WPRackCfg(token string, rack ctlplfl.Rack) (*funclib.FuncIntrm, error) {
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.WPRackCfg); err != nil {
		log.Errorf("WPRackCfg: auth failure: %v", err)
		return nil, err
	}
	resp := &ctlplfl.ResponseXML{
		Name:    rack.ID,
		Success: true,
	}
	commitChgs := srvctlplanefuncs.PopulateEntities(&rack, srvctlplanefuncs.RackPopulator{}, srvctlplanefuncs.RackKey)
	funcIntrm := funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: resp,
	}

	return &funcIntrm, nil
}

func (s *RackService) ReadRackCfg(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error) {
	key := srvctlplanefuncs.RackKey
	if !req.GetAll {
		key = srvctlplanefuncs.GetConfKey(srvctlplanefuncs.RackKey, req.ID)
	}
	if _, err := s.ValidateAndAuthorizeRBAC(token, authz.ReadRackCfg); err != nil {
		log.Errorf("ReadRackCfg: auth failure: %v", err)
		return ctlplfl.AuthError(err)
	}

	readResult, err := s.repo.RangeReadPrefix(cbArgs.Store, key, cbArgs.ReplySize)
	if err != nil {
		log.Error("Range read failure ", err)
		return ctlplfl.FuncError(err)
	}
	rackList := srvctlplanefuncs.ParseEntities[ctlplfl.Rack](readResult.ResultMap, srvctlplanefuncs.RackParser{})
	log.Debugf("ReadRackCfg: returning %d rack config(s) for key %s", len(rackList), key)
	return ctlplfl.EncodeResponse(rackList)
}

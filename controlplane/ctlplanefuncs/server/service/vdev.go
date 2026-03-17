package service

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/btree"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"

	auth "github.com/00pauln00/niova-mdsvc/controlplane/auth/jwt"
	authz "github.com/00pauln00/niova-mdsvc/controlplane/authorizer"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	srvctlplanefuncs "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server"
	"github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server/repository"

	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
)

type VdevService struct {
	*BaseService
	repo *repository.StorageRepository
}

func NewVdevService(repo *repository.StorageRepository, base *BaseService) *VdevService {
	return &VdevService{BaseService: base, repo: repo}
}

func (s *VdevService) WritePrep(token string, req ctlplfl.VdevReq) (*funclib.FuncIntrm, error) {
	tc, err := s.ValidateAndAuthorizeRBAC(token, authz.WPCreateVdev)
	if err != nil {
		return nil, err
	}
	if err := req.Vdev.Init(); err != nil {
		return nil, err
	}
	log.Infof("initializing vdev: %+v for user: %s", req.Vdev, tc.UserID)

	key := srvctlplanefuncs.GetConfKey(srvctlplanefuncs.VdevKey, req.Vdev.ID)
	commitChgs := make([]funclib.CommitChg, 0)
	for _, field := range []string{srvctlplanefuncs.SIZE, srvctlplanefuncs.NUM_CHUNKS, srvctlplanefuncs.NUM_REPLICAS} {
		var value string
		switch field {
		case srvctlplanefuncs.SIZE:
			value = strconv.Itoa(int(req.Vdev.Size))
		case srvctlplanefuncs.NUM_CHUNKS:
			value = strconv.Itoa(int(req.Vdev.NumChunks))
		case srvctlplanefuncs.NUM_REPLICAS:
			value = strconv.Itoa(int(req.Vdev.NumReplica))
		}
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   fmt.Appendf(nil, "%s/%s/%s", key, srvctlplanefuncs.CfgKey, field),
			Value: []byte(value),
		})
	}
	ownershipKey := fmt.Sprintf("/u/%s/v/%s", tc.UserID, req.Vdev.ID)
	commitChgs = append(commitChgs, funclib.CommitChg{
		Key:   []byte(ownershipKey),
		Value: []byte("1"),
	})
	return &funclib.FuncIntrm{
		Changes:  commitChgs,
		Response: ctlplfl.ResponseXML{ID: req.Vdev.ID},
	}, nil
}

func (s *VdevService) Apply(cbArgs *pumiceserver.PmdbCbArgs, req ctlplfl.VdevReq, intrm funclib.FuncIntrm) (interface{}, error) {
	// Forward write-prep error responses unchanged.
	if cpResp, ok := intrm.Response.(ctlplfl.CPResp); ok && cpResp.Error != nil {
		log.Debugf("Apply: forwarding write-prep error: %s", cpResp.Error.Message)
		return pmCmn.Encoder(pmCmn.GOB, cpResp)
	}
	resp, ok := intrm.Response.(ctlplfl.ResponseXML)
	if !ok {
		return ctlplfl.FuncError(fmt.Errorf("invalid response type in decoded intermediate data"))
	}
	req.Vdev.ID = resp.ID
	req.Vdev.NumChunks = uint32(ctlplfl.Count8GBChunks(req.Vdev.Size))

	allocMap := btree.NewMap[string, *ctlplfl.NisdVdevAlloc](32)
	defer allocMap.Clear()
	srvctlplanefuncs.HR.Dump()

	if err := srvctlplanefuncs.AllocNISDs(&req, allocMap, &intrm); err != nil {
		log.Errorf("Apply: NISD allocation failed for vdev %s: %v", req.Vdev.ID, err)
		return ctlplfl.FuncError(err)
	}
	resp.Success = true

	// Append NISD available-space refund changes, then commit everything.
	allocMap.Ascend("", func(_ string, alloc *ctlplfl.NisdVdevAlloc) bool {
		intrm.Changes = append(intrm.Changes, funclib.CommitChg{
			Key:   fmt.Appendf(nil, "%s/%s", srvctlplanefuncs.GetConfKey(srvctlplanefuncs.NisdCfgKey, alloc.Ptr.ID), srvctlplanefuncs.AVAIL_SPACE),
			Value: []byte(strconv.Itoa(int(alloc.AvailableSize))),
		})
		return true
	})
	if err := s.repo.ApplyChanges(cbArgs.Store, intrm.Changes); err != nil {
		log.Errorf("Apply: failed to commit allocation changes for vdev %s: %v", req.Vdev.ID, err)
		return ctlplfl.FuncError(err)
	}

	// Update in-memory NISD available space and prune exhausted NISDs.
	allocMap.Ascend("", func(_ string, alloc *ctlplfl.NisdVdevAlloc) bool {
		alloc.Ptr.AvailableSize = alloc.AvailableSize
		if alloc.Ptr.AvailableSize < ctlplfl.CHUNK_SIZE {
			srvctlplanefuncs.HR.DeleteNisd(alloc.Ptr)
		}
		return true
	})
	srvctlplanefuncs.HR.Dump()
	return ctlplfl.EncodeResponse(resp)
}

func (s *VdevService) Delete(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.DeleteVdevReq) (interface{}, error) {
	if err := req.Validate(); err != nil {
		return ctlplfl.FuncError(err)
	}
	tc, err := s.ValidateAndAuthorizeRBAC(token, authz.APDeleteVdev)
	if err != nil {
		return ctlplfl.AuthError(err)
	}

	resp := ctlplfl.ResponseXML{Name: "vdev", ID: req.ID, Success: false}
	commitChgs := make([]funclib.CommitChg, 0)
	deleteChgs := make([]funclib.CommitChg, 0)
	nisdRefundMap := make(map[string]*ctlplfl.Nisd)

	vKey := srvctlplanefuncs.GetConfKey(srvctlplanefuncs.VdevKey, req.ID)
	rrArgs := storageiface.RangeReadArgs{
		Selector: srvctlplanefuncs.Colmfamily,
		Key:      vKey,
		BufSize:  cbArgs.ReplySize,
		Prefix:   vKey,
	}
	for {
		rrOp, err := cbArgs.Store.RangeRead(rrArgs)
		if err != nil {
			log.Error("Range read failure: ", err)
			return ctlplfl.FuncError(err)
		}
		for k, v := range rrOp.ResultMap {
			deleteChgs = append(deleteChgs, funclib.CommitChg{Key: []byte(k)})
			if strings.Contains(k, "/c/") && strings.Contains(k, "/R.") {
				nisdID := string(v)
				if _, ok := nisdRefundMap[nisdID]; !ok {
					nKey := srvctlplanefuncs.GetConfKey(srvctlplanefuncs.NisdCfgKey, nisdID)
					nisdRR, err := cbArgs.Store.RangeRead(storageiface.RangeReadArgs{
						Selector:   srvctlplanefuncs.Colmfamily,
						Key:        nKey,
						BufSize:    cbArgs.ReplySize,
						Consistent: false,
						Prefix:     nKey,
					})
					if err != nil {
						return ctlplfl.FuncError(err)
					}
					nisdList := srvctlplanefuncs.ParseEntities[ctlplfl.Nisd](nisdRR.ResultMap, srvctlplanefuncs.NisdParser{})
					if len(nisdList) > 0 {
						nisdRefundMap[nisdID] = &nisdList[0]
					}
				}
				if nisd, ok := nisdRefundMap[nisdID]; ok {
					nisd.AvailableSize += ctlplfl.CHUNK_SIZE
				}
				revKey := fmt.Sprintf("%s/%s/%s", srvctlplanefuncs.NisdKey, nisdID, req.ID)
				deleteChgs = append(deleteChgs, funclib.CommitChg{Key: []byte(revKey)})
			}
		}
		if rrOp.LastKey == "" {
			break
		}
		rrArgs.Key = rrOp.LastKey
		rrArgs.Prefix = vKey
		rrArgs.SeqNum = rrOp.SeqNum
	}

	ownershipKey := fmt.Sprintf("/u/%s/v/%s", tc.UserID, req.ID)
	deleteChgs = append(deleteChgs, funclib.CommitChg{Key: []byte(ownershipKey)})

	for nisdID, nisd := range nisdRefundMap {
		asKey := fmt.Sprintf("%s/%s", srvctlplanefuncs.GetConfKey(srvctlplanefuncs.NisdCfgKey, nisdID), srvctlplanefuncs.AVAIL_SPACE)
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   []byte(asKey),
			Value: []byte(strconv.FormatInt(nisd.AvailableSize, 10)),
		})
	}
	if err := s.repo.ApplyChanges(cbArgs.Store, commitChgs); err != nil {
		return ctlplfl.FuncError(err)
	}
	if err := s.repo.DeleteChanges(cbArgs.Store, deleteChgs); err != nil {
		return ctlplfl.FuncError(err)
	}
	for nisdID, nisd := range nisdRefundMap {
		hrNisd, err := srvctlplanefuncs.HR.GetNisdByPDUID(nisd.FailureDomain[ctlplfl.PDU_IDX], nisdID)
		if err != nil {
			return ctlplfl.FuncError(err)
		}
		hrNisd.AvailableSize = nisd.AvailableSize
	}
	resp.Success = true
	return ctlplfl.EncodeResponse(resp)
}

func (s *VdevService) ReadInfo(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error) {
	if err := req.ValidateRequest(); err != nil {
		return ctlplfl.FuncError(fmt.Errorf("Invalid Request"))
	}
	tc, err := srvctlplanefuncs.ValidateToken(token)
	if err != nil {
		return ctlplfl.AuthError(fmt.Errorf("Invalid Token"))
	}
	if s.authorizer != nil {
		attributes := map[string]string{"vdev": req.ID}
		if !s.authorizer.Authorize(authz.ReadVdevInfo, tc.UserID, []string{tc.Role}, attributes, cbArgs.Store, srvctlplanefuncs.Colmfamily) {
			return ctlplfl.AuthError(fmt.Errorf("User is not authorized"))
		}
	}

	vKey := srvctlplanefuncs.GetConfKey(srvctlplanefuncs.VdevKey, req.ID)
	rqResult, err := s.repo.RangeReadPrefix(cbArgs.Store, vKey, cbArgs.ReplySize)
	if err != nil {
		return ctlplfl.FuncError(err)
	}

	authtc := &auth.Token{Secret: []byte(ctlplfl.NISD_SECRET), TTL: time.Minute}
	authtoken, err := authtc.CreateToken(map[string]any{"vdevID": req.ID})
	if err != nil {
		return ctlplfl.FuncError(err)
	}

	vdevInfo := ctlplfl.VdevCfg{ID: req.ID, AuthToken: authtoken}
	for k, v := range rqResult.ResultMap {
		parts := strings.Split(strings.Trim(k, "/"), "/")
		if parts[srvctlplanefuncs.BASE_UUID_PREFIX] != vdevInfo.ID {
			continue
		}
		if parts[srvctlplanefuncs.VDEV_CFG_C_KEY] != srvctlplanefuncs.CfgKey {
			continue
		}
		switch parts[srvctlplanefuncs.VDEV_ELEMENT_KEY] {
		case srvctlplanefuncs.SIZE:
			if sz, err := strconv.ParseInt(string(v), 10, 64); err == nil {
				vdevInfo.Size = sz
			}
		case srvctlplanefuncs.NUM_CHUNKS:
			if nc, err := strconv.ParseUint(string(v), 10, 32); err == nil {
				vdevInfo.NumChunks = uint32(nc)
			}
		case srvctlplanefuncs.NUM_REPLICAS:
			if nr, err := strconv.ParseUint(string(v), 10, 8); err == nil {
				vdevInfo.NumReplica = uint8(nr)
			}
		}
	}
	return ctlplfl.EncodeResponse(vdevInfo)
}

func (s *VdevService) ReadAllInfo(cbArgs *pumiceserver.PmdbCbArgs) (interface{}, error) {
	rqResult, err := s.repo.RangeReadPrefix(cbArgs.Store, srvctlplanefuncs.VdevKey, cbArgs.ReplySize)
	if err != nil {
		return ctlplfl.FuncError(err)
	}
	vdevList := srvctlplanefuncs.ParseEntities[ctlplfl.VdevCfg](rqResult.ResultMap, srvctlplanefuncs.VdevParser{})
	return ctlplfl.EncodeResponse(vdevList)
}

func (s *VdevService) ReadInfoWithChunkMapping(cbArgs *pumiceserver.PmdbCbArgs, token string, req ctlplfl.GetReq) (interface{}, error) {
	tc, err := srvctlplanefuncs.ValidateToken(token)
	if err != nil {
		return ctlplfl.FuncError(err)
	}
	if s.authorizer != nil {
		if req.GetAll {
			if !s.authorizer.Authorize(authz.ReadAllVdevInfo, tc.UserID, []string{tc.Role}, map[string]string{}, nil, "") {
				return ctlplfl.AuthError(fmt.Errorf("User is not authorized"))
			}
		} else {
			attributes := map[string]string{"vdev": req.ID}
			if !s.authorizer.Authorize(authz.ReadVdevsInfoWithChunkMapping, tc.UserID, []string{tc.Role}, attributes, cbArgs.Store, srvctlplanefuncs.Colmfamily) {
				return ctlplfl.AuthError(fmt.Errorf("User is not authorized"))
			}
		}
	}

	nisdResult, err := s.repo.RangeReadPrefix(cbArgs.Store, srvctlplanefuncs.NisdCfgKey, cbArgs.ReplySize)
	if err != nil {
		return ctlplfl.FuncError(err)
	}
	nisdEntityMap := srvctlplanefuncs.ParseEntitiesMap(nisdResult.ResultMap, srvctlplanefuncs.NisdParser{})

	key := srvctlplanefuncs.VdevKey
	if !req.GetAll {
		key = srvctlplanefuncs.GetConfKey(srvctlplanefuncs.VdevKey, req.ID)
	}
	readResult, err := s.repo.RangeReadPrefix(cbArgs.Store, key, cbArgs.ReplySize)
	if err != nil {
		return ctlplfl.FuncError(err)
	}

	vdevMap := make(map[string]*ctlplfl.Vdev)
	vdevNisdChunkMap := make(map[string]map[string]*ctlplfl.NisdChunk)

	for k, value := range readResult.ResultMap {
		parts := strings.Split(strings.Trim(k, "/"), "/")
		vdevID := parts[srvctlplanefuncs.BASE_UUID_PREFIX]
		if _, ok := vdevMap[vdevID]; !ok {
			vdevMap[vdevID] = &ctlplfl.Vdev{Cfg: ctlplfl.VdevCfg{ID: vdevID}}
		}
		vdev := vdevMap[vdevID]
		switch parts[srvctlplanefuncs.VDEV_CFG_C_KEY] {
		case srvctlplanefuncs.CfgKey:
			switch parts[srvctlplanefuncs.VDEV_ELEMENT_KEY] {
			case srvctlplanefuncs.SIZE:
				if sz, err := strconv.ParseInt(string(value), 10, 64); err == nil {
					vdev.Cfg.Size = sz
				}
			case srvctlplanefuncs.NUM_CHUNKS:
				if nc, err := strconv.ParseUint(string(value), 10, 32); err == nil {
					vdev.Cfg.NumChunks = uint32(nc)
				}
			case srvctlplanefuncs.NUM_REPLICAS:
				if nr, err := strconv.ParseUint(string(value), 10, 8); err == nil {
					vdev.Cfg.NumReplica = uint8(nr)
				}
			}
		case srvctlplanefuncs.ChunkKey:
			nisdID := string(value)
			if _, ok := vdevNisdChunkMap[vdevID]; !ok {
				vdevNisdChunkMap[vdevID] = make(map[string]*ctlplfl.NisdChunk)
			}
			perVdevMap := vdevNisdChunkMap[vdevID]
			if _, ok := perVdevMap[nisdID]; !ok {
				nc := &ctlplfl.NisdChunk{Chunk: make([]int, 0)}
				if ent, ok := nisdEntityMap[nisdID]; ok {
					if nisdPtr, ok := ent.(*ctlplfl.Nisd); ok {
						nc.Nisd = *nisdPtr
					} else {
						nc.Nisd = ctlplfl.Nisd{ID: nisdID}
					}
				} else {
					nc.Nisd = ctlplfl.Nisd{ID: nisdID}
				}
				perVdevMap[nisdID] = nc
			}
			if len(parts) > 3 {
				if chunkIdx, err := strconv.Atoi(parts[3]); err == nil {
					perVdevMap[nisdID].Chunk = append(perVdevMap[nisdID].Chunk, chunkIdx)
				}
			}
		}
	}

	vdevList := make([]ctlplfl.Vdev, 0, len(vdevMap))
	for vid, v := range vdevMap {
		if perVdevMap, ok := vdevNisdChunkMap[vid]; ok {
			for _, nc := range perVdevMap {
				v.NisdToChkMap = append(v.NisdToChkMap, *nc)
			}
		}
		vdevList = append(vdevList, *v)
	}
	return ctlplfl.EncodeResponse(vdevList)
}

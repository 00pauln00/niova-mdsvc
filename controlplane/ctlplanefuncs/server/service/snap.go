package service

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	"github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/server/repository"

	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

type SnapService struct {
	*BaseService
	repo *repository.StorageRepository
}

func NewSnapService(repo *repository.StorageRepository, base *BaseService) *SnapService {
	return &SnapService{BaseService: base, repo: repo}
}

func (s *SnapService) WritePrepCreateSnap(snap ctlplfl.SnapXML) (*funclib.FuncIntrm, error) {
	commitChgs := make([]funclib.CommitChg, 0)
	for _, chunk := range snap.Chunks {
		// Schema: {vdev}/snap/{chunk}/{Seq} : {Ref count}
		commitChgs = append(commitChgs, funclib.CommitChg{
			Key:   fmt.Appendf(nil, "%s/snap/%d/%d", snap.Vdev, chunk.Idx, chunk.Seq),
			Value: []byte{uint8(1)},
		})
	}
	// Schema: snap/{name}: {blob}
	blob, err := pmCmn.Encoder(pmCmn.GOB, snap)
	if err != nil {
		return nil, fmt.Errorf("failed to encode snap blob: %v", err)
	}
	commitChgs = append(commitChgs, funclib.CommitChg{
		Key:   fmt.Appendf(nil, "snap/%s", snap.SnapName),
		Value: blob,
	})
	snapResponse := ctlplfl.SnapResponseXML{
		SnapName: ctlplfl.SnapName{
			Name:    snap.SnapName,
			Success: true,
		},
	}
	// Response is a concrete struct: wildcard ApplyFunc calls EncodeResponse(intrm.Response)
	return &funclib.FuncIntrm{Changes: commitChgs, Response: snapResponse}, nil
}

func (s *SnapService) ReadSnapByName(cbArgs *pumiceserver.PmdbCbArgs, snap ctlplfl.SnapXML) (interface{}, error) {
	key := fmt.Sprintf("snap/%s", snap.SnapName)
	log.Info("Key to be read: ", key)
	readResult, err := s.repo.Read(cbArgs.Store, key)
	if err != nil {
		log.Error("Read failure ", err)
		return ctlplfl.FuncError(err)
	}
	log.Debugf("ReadSnapByName: returning snap data for key %s", key)
	return ctlplfl.EncodeResponse(readResult)
}

func (s *SnapService) ReadSnapForVdev(cbArgs *pumiceserver.PmdbCbArgs, snap ctlplfl.SnapXML) (interface{}, error) {
	key := fmt.Sprintf("%s/snap", snap.Vdev)
	readResult, err := s.repo.RangeReadPrefix(cbArgs.Store, key, cbArgs.ReplySize)
	if err != nil {
		log.Error("Range read failure ", err)
		return ctlplfl.FuncError(err)
	}
	log.Tracef("ReadSnapForVdev: range read returned %d results for vdev %s", len(readResult.ResultMap), snap.Vdev)
	for k := range readResult.ResultMap {
		c := strings.Split(k, "/")
		idx, err1 := strconv.ParseUint(c[len(c)-2], 10, 32)
		seq, err2 := strconv.ParseUint(c[len(c)-1], 10, 64)
		if err1 != nil || err2 != nil {
			log.Errorf("ReadSnapForVdev: failed to parse chunk index/seq from key %q", k)
			return ctlplfl.FuncError(fmt.Errorf("failed to parse chunk index/seq from key %q", k))
		}
		snap.Chunks = append(snap.Chunks, ctlplfl.ChunkXML{
			Idx: uint32(idx),
			Seq: seq,
		})
	}
	log.Debugf("ReadSnapForVdev: returning snap info for vdev %s with %d chunks", snap.Vdev, len(snap.Chunks))
	return ctlplfl.EncodeResponse(snap)
}

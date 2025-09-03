package clictlplanefuncs

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	sd "github.com/00pauln00/niova-pumicedb/go/pkg/utils/servicediscovery"
)

type EncodeType int

const (
	XML EncodeType = iota
	GoBinary
)

// Client side interferace for control plane functions
type CliCFuncs struct {
	appUUID  string
	writeSeq uint64
	sdObj    *sd.ServiceDiscoveryHandler
	encType  EncodeType
}

func InitCliCFuncs(appUUID string, key string, gossipConfigPath string) *CliCFuncs {
	ccf := CliCFuncs{
		appUUID:  appUUID,
		writeSeq: uint64(0),
		encType:  XML, // Default encoding type
	}

	ccf.sdObj = &sd.ServiceDiscoveryHandler{
		HTTPRetry: 10,
		SerfRetry: 5,
		RaftUUID:  key,
	}
	stop := make(chan int)
	log.Info("Staring Client API using gossip path: ", gossipConfigPath)
	go ccf.sdObj.StartClientAPI(stop, gossipConfigPath)

	log.Info("Successfully initialized controlplane client: ", appUUID)
	return &ccf
}

func (ccf *CliCFuncs) encode(data interface{}) ([]byte, error) {
	if ccf.encType == GoBinary {
		return pmCmn.GobEncode(data)
	} else if ccf.encType == XML {
		return ctlplfl.XMLEncode(data)
	}
	return nil, errors.New("unsupported encoding type")
}

func (ccf *CliCFuncs) decode(bin []byte, st interface{}) error {
	if ccf.encType == GoBinary {
		return pmCmn.GobDecode(bin, st)
	} else if ccf.encType == XML {
		return ctlplfl.XMLDecode(bin, st)
	}
	return errors.New("unsupported decoding type")
}

func (ccf *CliCFuncs) request(rqb []byte, urla string, isWrite bool) ([]byte, error) {
	ccf.sdObj.TillReady("PROXY", 5)
	rsp, err := ccf.sdObj.Request(rqb, "/func?"+urla, isWrite)
	if err != nil {
		log.Error("failed to send request to server: ", err)
		return nil, err
	}
	return rsp, nil
}

func (ccf *CliCFuncs) _put(urla string, rqb []byte) ([]byte, error) {
	rncui := fmt.Sprintf("%s:0:0:0:%d", ccf.appUUID, ccf.writeSeq)
	ccf.writeSeq += 1
	urla += "&rncui=" + rncui
	rsb, err := ccf.request(rqb, urla, true)

	return rsb, err
}

func (ccf *CliCFuncs) put(data interface{}, urla string) error {
	url := "name=" + urla
	rqb, err := ccf.encode(data)
	if err != nil {
		log.Error("failed to encode data: ", err)
		return err
	}

	rsb, err := ccf._put(url, rqb)
	if err != nil {
		log.Error("failed to send request(_put): ", err)
		return err
	}

	var resp ctlplfl.ResponseXML
	err = ccf.decode(rsb, &resp)
	if err != nil {
		log.Error("failed to decode response: ", err)
		return err
	}

	return nil
}

func (ccf *CliCFuncs) get(data interface{}, urla string) error {
	url := "name=" + urla
	rqb, err := ccf.encode(data)
	if err != nil {
		log.Error("failed to encode data: ", err)
		return err
	}

	rsb, err := ccf.request(rqb, url, false)
	if err != nil {
		log.Error("request failed: ", err)
		return err
	}
	err = ccf.decode(rsb, data)
	if err != nil {
		log.Error("failed to decode response: ", err)
		return err
	}
	return nil
}

func (ccf *CliCFuncs) CreateSnap(vdev string, chunkSeq []uint64, snapName string) error {
	urla := "name=CreateSnap"

	chks := make([]ctlplfl.ChunkXML, 0)
	for idx, seq := range chunkSeq {
		chks = append(chks,
			ctlplfl.ChunkXML{
				Idx: uint32(idx),
				Seq: seq,
			})
	}
	var Snap ctlplfl.SnapXML
	Snap.SnapName = snapName
	Snap.Vdev = vdev
	Snap.Chunks = chks

	rqb, err := ccf.encode(Snap)
	if err != nil {
		return err
	}

	rsb, err := ccf._put(urla, rqb)
	if err != nil {
		return err
	}

	var snapRes ctlplfl.SnapResponseXML
	err = ccf.decode(rsb, snapRes)
	if err != nil {
		return err
	}

	if !snapRes.SnapName.Success {
		return errors.New("Snap not created")
	}

	return nil
}

func (ccf *CliCFuncs) ReadSnapByName(name string) ([]byte, error) {
	urla := "name=ReadSnapByName"

	var snap ctlplfl.SnapXML
	snap.SnapName = name
	rqb, err := ccf.encode(snap)
	if err != nil {
		return nil, err
	}

	return ccf.request(rqb, urla, false)
}

func (ccf *CliCFuncs) ReadSnapForVdev(vdev string) ([]byte, error) {
	urla := "name=ReadSnapForVdev"

	var snap ctlplfl.SnapXML
	snap.Vdev = vdev
	rqb, err := ccf.encode(snap)
	if err != nil {
		return nil, err
	}

	return ccf.request(rqb, urla, false)
}

func (ccf *CliCFuncs) PutDeviceCfg(device *ctlplfl.DeviceInfo) error {
	return ccf.put(device, ctlplfl.PUT_DEVICE)
}

func (ccf *CliCFuncs) GetDeviceCfg(dev *ctlplfl.DeviceInfo) error {
	return ccf.get(dev, ctlplfl.GET_DEVICE)
}

func (ccf *CliCFuncs) PutNisdCfg(ncfg *ctlplfl.Nisd) error {
	return ccf.put(ncfg, ctlplfl.PUT_NISD)
}

func (ccf *CliCFuncs) GetNisdCfg(ncfg *ctlplfl.Nisd) error {
	return ccf.get(ncfg, ctlplfl.GET_NISD)
}

func (ccf *CliCFuncs) CreateVdev(vdev *ctlplfl.Vdev) error {
	return ccf.put(vdev, ctlplfl.CREATE_VDEV)
}

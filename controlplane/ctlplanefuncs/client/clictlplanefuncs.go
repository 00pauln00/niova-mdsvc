package clictlplanefuncs

import (
 "fmt"
 sd "github.com/00pauln00/niova-pumicedb/go/pkg/utils/servicediscovery"
 ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
 "encoding/xml"
)

//Client side interferace for control plane functions
type CliCFuncs struct { 
 appUUID string
 writeSeq uint64
 sdObj sd.ServiceDiscoveryHandler
 writePathLock sync.Mutex
}


func InitCliCFuncs(appUUID string, gossipConfigPath string) *CliCFuns {
    ccf := CliCFuns {
        appUUID : appUUID,
        writeSeq : uint64(0),
    }

    ccf.sdObj = &sd.ServiceDiscoveryHandler{
		HTTPRetry: 10,
		SerfRetry: 5,
		RaftUUID:  key,
	}
	stop := make(chan int)
	go ccf.sdObj.StartClientAPI(stop, gossipConfigPath)
    
    return &ccf
}


func (ccf *CliCFuncs) request(rqb []byte, urla string, isWrite bool) {
    ccf.sdObj.TillReady("PROXY", 5)
    rsp, err := ccf.sdObj.Request(rqb, "/func?" + urla, isWrite)
    if err != nil {
        return nil, err
    }
    return rsp, nil
}

func encode(data *interface{}) ([]byte, error) {
    return xml.Marshal(data)
}

func (ccf *CliCFuns) CreateSnap(vdev string, chunkSeq []uint32, snapName string) error {
    urla := "name=CreateSnap"

    chks := make(ctlplfl.ChunkXML)
    for _, seq := range chunkSeq {
        chks = append(chks, seq)
    }
    var Snap ctlplfl.SnapXML
    Snap.SnapName = snapName
    Snap.Vdevs = append(Snap.Vdevs, ctlplfl.VdevXML{
        VdevName : vdev,
        Chunks: chks,
    })

    rqb, err := encode(Snap)
    if err != nil {
        return err
    }

    ccf.writePathLock.Lock()
    rncui := ccf.appUUID + "0:0:0:" + str(ccf.writeSeq)
    ccf.writeSeq += 1
    rsb, err := ccf.request(rqb, urla, true)
    ccf.writePathLock.Unlock()

    retun nil
}


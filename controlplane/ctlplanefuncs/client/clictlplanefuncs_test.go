package clictlplanefuncs

import (
	"os"
	"testing"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func newClient(t *testing.T) *CliCFuncs {

	clusterID := os.Getenv("RAFT_ID")
	if clusterID == "" {
		log.Fatal("RAFT_ID env variable not set")
	}

	configPath := os.Getenv("GOSSIP_NODES_PATH")
	if configPath == "" {
		log.Fatal("GOSSIP_NODES_PATH env variable not set")
	}
	c := InitCliCFuncs(
		uuid.New().String(),
		clusterID,
		configPath,
	)
	if c == nil {
		t.Fatal("failed to init client funcs")
	}
	return c
}

func TestPutAndGetPDU(t *testing.T) {
	c := newClient(t)

	pdus := []cpLib.PDU{
		{ID: "95f62aee-997e-11f0-9f1b-a70cff4b660b"},
		{ID: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea"},
	}

	for _, p := range pdus {
		resp, err := c.PutPDU(&p)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	res, err := c.GetPDUs(&cpLib.GetReq{GetAll: true})
	log.Info("GetPDUs: ", res)
	assert.NoError(t, err)
}

func TestPutAndGetRack(t *testing.T) {
	c := newClient(t)

	racks := []cpLib.Rack{
		{ID: "rack-1", PDUId: "95f62aee-997e-11f0-9f1b-a70cff4b660b"},
		{ID: "rack-2", PDUId: "13ce1c48-9979-11f0-8bd0-4f62ec9356ea"},
	}

	for _, r := range racks {
		resp, err := c.PutRack(&r)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	resp, err := c.GetRacks(&cpLib.GetReq{GetAll: true})
	log.Info("GetRacks: ", resp)
	assert.NoError(t, err)
}

func TestPutAndGetHypervisor(t *testing.T) {
	c := newClient(t)

	hypervisors := []cpLib.Hypervisor{
		{RackID: "rack-1", ID: "hv-1", IPAddress: "127.0.0.1"},
		{RackID: "rack-2", ID: "hv-2", IPAddress: "127.0.0.2"},
	}

	for _, hv := range hypervisors {
		resp, err := c.PutHypervisor(&hv)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	}

	resp, err := c.GetHypervisor(&cpLib.GetReq{GetAll: true})
	log.Info("GetHypervisor: ", resp)
	assert.NoError(t, err)
}

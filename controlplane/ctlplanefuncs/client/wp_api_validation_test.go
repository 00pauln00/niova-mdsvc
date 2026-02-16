package clictlplanefuncs

import (
	"fmt"
	"testing"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestAPI_PDUValid(t *testing.T) {
	c := newClient(t)

	pdu := cpLib.PDU{
		ID:            "95f62aee-997e-11f0-9f1b-a70cff4b660b",
		Name:          "pdu-1",
		Location:      "us-west",
		PowerCapacity: "15Kw",
		Specification: "specification1",
	}

	resp, err := c.PutPDU(&pdu)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
}

func TestAPIGetPDUs_All(t *testing.T) {
	c := newClient(t)
	res, err := c.GetPDUs(&cpLib.GetReq{GetAll: true})
	assert.NoError(t, err)
	assert.Len(t, res, 1)
}

func TestAPIPutPDU_InvalidUUID(t *testing.T) {
	c := newClient(t)

	pdu := cpLib.PDU{
		ID:   "invalid-uuid",
		Name: "bad-pdu",
	}

	// TODO: pending error handling
	resp, _ := c.PutPDU(&pdu)

	assert.False(t, resp.Success)
}

func TestAPIAPIPutPDU_DuplicateUUID(t *testing.T) {
	c := newClient(t)

	pdu := cpLib.PDU{
		ID:   "e43aae8a-023f-11f1-bb31-9368c7f60be4",
		Name: "pdu-3",
	}

	_, err := c.PutPDU(&pdu)
	assert.NoError(t, err)

	resp, err := c.PutPDU(&pdu)
	assert.False(t, resp.Success)
}

func TestAPIRack_ValidUUID(t *testing.T) {
	c := newClient(t)

	r := cpLib.Rack{
		ID:    "8a5303ae-ab23-11f0-bb87-632ad3e09c04",
		PDUID: "95f62aee-997e-11f0-9f1b-a70cff4b660b",
		Name:  "rack-1",
	}

	_, err := c.PutRack(&r)
	assert.NoError(t, err)

	res, err := c.GetRacks(&cpLib.GetReq{ID: r.ID})
	assert.NoError(t, err)
	assert.Equal(t, r.ID, res[0].ID)
}

func TestAPIRack_InvalidUUID(t *testing.T) {
	c := newClient(t)

	r := cpLib.Rack{ID: "bad-uuid"}
	resp, _ := c.PutRack(&r)

	assert.False(t, resp.Success)

	r = cpLib.Rack{ID: "8a5303ae-ab23-11f0-bb87-632ad3e09c04", PDUID: "bad-uuid"}
	resp, _ = c.PutRack(&r)

	assert.False(t, resp.Success)
}

func TestAPIRack_DuplicateUUID(t *testing.T) {
	c := newClient(t)

	r := cpLib.Rack{ID: "158011be-0241-11f1-9c04-3bdbd9aafd88",
		PDUID: "95f62aee-997e-11f0-9f1b-a70cff4b660b",
		Name:  "rack-1"}
	resp, err := c.PutRack(&r)
	fmt.Println("resp: ", resp, err)
	assert.True(t, resp.Success)

	resp, err = c.PutRack(&r)
	fmt.Println("resp: ", resp, err)
	assert.False(t, resp.Success)
}

func TestAPIHypervisor_ValidUUID(t *testing.T) {
	c := newClient(t)

	hv := cpLib.Hypervisor{
		ID:     "89944570-ab2a-11f0-b55d-8fc2c05d35f4",
		RackID: "8a5303ae-ab23-11f0-bb87-632ad3e09c04",
		Name:   "hv-1",
	}

	_, err := c.PutHypervisor(&hv)
	assert.NoError(t, err)

	res, err := c.GetHypervisor(&cpLib.GetReq{ID: hv.ID})
	assert.NoError(t, err)
	assert.Equal(t, hv.ID, res[0].ID)
}

func TestAPIHypervisor_InvalidUUID(t *testing.T) {
	c := newClient(t)

	hv := cpLib.Hypervisor{ID: "bad-uuid"}
	resp, _ := c.PutHypervisor(&hv)

	assert.False(t, resp.Success)
}

func TestAPIHypervisor_DuplicateUUID(t *testing.T) {
	c := newClient(t)

	hv := cpLib.Hypervisor{ID: "8f70f2a4-ab2a-11f0-a1bb-cb25e1fa6a6b"}
	_, _ = c.PutHypervisor(&hv)

	resp, _ := c.PutHypervisor(&hv)
	assert.False(t, resp.Success)
}

func TestAPIPartition_ValidUUID(t *testing.T) {
	c := newClient(t)

	p := cpLib.DevicePartition{
		PartitionID: "dev-1-part1",
		DevID:       "dev-1",
		Size:        1024,
		NISDUUID:    uuid.NewString(),
	}

	_, err := c.PutPartition(&p)
	assert.NoError(t, err)

	res, err := c.GetPartition(cpLib.GetReq{ID: p.PartitionID})

	assert.NoError(t, err)
	assert.Equal(t, p.PartitionID, res[0].PartitionID)
}

func TestAPIPartition_InvalidUUID(t *testing.T) {
	c := newClient(t)

	p := cpLib.DevicePartition{PartitionID: "bad-uuid"}
	resp, _ := c.PutPartition(&p)
	assert.False(t, resp.Success)
}

func TestAPIPartition_DuplicateUUID(t *testing.T) {
	c := newClient(t)

	p := cpLib.DevicePartition{PartitionID: "dup-part"}
	_, _ = c.PutPartition(&p)

	resp, _ := c.PutPartition(&p)
	assert.False(t, resp.Success)
}

func TestAPIDevice_ValidUUID(t *testing.T) {
	c := newClient(t)

	d := cpLib.Device{
		ID:           "60447cd0-ab3e-11f0-aa15-1f40dd976538",
		SerialNumber: "SN123",
		HypervisorID: uuid.NewString(),
	}

	_, err := c.PutDevice(&d)
	assert.NoError(t, err)

	res, err := c.GetDevices(cpLib.GetReq{ID: d.ID})
	assert.NoError(t, err)
	assert.Equal(t, d.ID, res[0].ID)
}

func TestAPIDevice_InvalidUUID(t *testing.T) {
	c := newClient(t)

	d := cpLib.Device{ID: "bad-uuid"}
	resp, _ := c.PutDevice(&d)
	assert.False(t, resp.Success)
}

func TestAPIDevice_DuplicateUUID(t *testing.T) {
	c := newClient(t)

	d := cpLib.Device{ID: "60447cd0-ab3e-11f0-aa15-1f40dd976538"}
	_, _ = c.PutDevice(&d)

	resp, _ := c.PutDevice(&d)
	assert.False(t, resp.Success)
}

func TestAPINisd_ValidUUID(t *testing.T) {
	c := newClient(t)

	n := cpLib.Nisd{
		PeerPort: 8001,
		ID:       "3355858e-ea0e-11f0-9768-e71fc352cd1d",
		FailureDomain: []string{
			"87694b06-ea0e-11f0-8fc4-e3e2449638d1",
			"87694b06-ea0e-11f0-8fc4-e3e2449638d2",
			"87694b06-ea0e-11f0-8fc4-e3e2449638d3",
			"nvme-e3f4123",
		},
		TotalSize:     1_000_000_000_000, // 1 TB
		AvailableSize: 750_000_000_000,   // 750 GB
		SocketPath:    "/path/sockets1",
		NetInfo: cpLib.NetInfoList{
			cpLib.NetworkInfo{
				IPAddr: "192.168.0.0.1",
				Port:   5444,
			},
			cpLib.NetworkInfo{
				IPAddr: "192.168.0.0.2",
				Port:   6444,
			},
		},
		NetInfoCnt: 2,
	}

	_, err := c.PutNisd(&n)
	assert.NoError(t, err)

	result, err := c.GetNisds()
	assert.NoError(t, err)
	assert.Equal(t, n.ID, result[0].ID)
}

func TestAPINisd_InvalidUUID(t *testing.T) {
	c := newClient(t)

	n := cpLib.Nisd{ID: "bad-uuid"}
	resp, _ := c.PutNisd(&n)

	assert.False(t, resp.Success)
}

func TestAPINisd_DuplicateUUID(t *testing.T) {
	c := newClient(t)

	n := cpLib.Nisd{
		PeerPort: 8001,
		ID:       uuid.NewString(),
		FailureDomain: []string{
			"87694b06-ea0e-11f0-8fc4-e3e2449638d1",
			"87694b06-ea0e-11f0-8fc4-e3e2449638d2",
			"87694b06-ea0e-11f0-8fc4-e3e2449638d3",
			"nvme-e3f4123",
		},
		TotalSize:     1_000_000_000_000, // 1 TB
		AvailableSize: 750_000_000_000,   // 750 GB
		SocketPath:    "/path/sockets1",
		NetInfo: cpLib.NetInfoList{
			cpLib.NetworkInfo{
				IPAddr: "192.168.0.0.1",
				Port:   5444,
			},
			cpLib.NetworkInfo{
				IPAddr: "192.168.0.0.2",
				Port:   6444,
			},
		},
		NetInfoCnt: 2,
	}
	resp, err := c.PutNisd(&n)
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	resp, _ = c.PutNisd(&n)
	assert.False(t, resp.Success)
}

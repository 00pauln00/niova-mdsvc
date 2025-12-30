package client

import (
	"fmt"
	"sync"
	"sync/atomic"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"

	auth "github.com/00pauln00/niova-mdsvc/controlplane/auth/lib"
	pmCmn "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	sd "github.com/00pauln00/niova-pumicedb/go/pkg/utils/servicediscovery"
)

const (
	defaultLogLevel    = "Info"
	defaultHTTPRetry   = 10
	defaultSerfRetry   = 5
	defaultEncodingFmt = pmCmn.JSON
)

// Config holds the configuration for new auth client
type Config struct {
	AppUUID          string
	RaftUUID         string
	GossipConfigPath string
	LogLevel         string
	LogFile          string
	EncodingFormat   pmCmn.Format
}

// Client represents an authentication service client
type Client struct {
	appUUID  string
	writeSeq uint64
	sd       *sd.ServiceDiscoveryHandler
	encType  pmCmn.Format
	stop     chan int
	once     sync.Once
}

// New creates and initializes a new authentication client
func New(cfg Config) (*Client, func()) {
	if cfg.LogLevel == "" {
		cfg.LogLevel = defaultLogLevel
	}
	if cfg.EncodingFormat == "" {
		cfg.EncodingFormat = defaultEncodingFmt
	}

	c := &Client{
		appUUID: cfg.AppUUID,
		sd: &sd.ServiceDiscoveryHandler{
			HTTPRetry: defaultHTTPRetry,
			SerfRetry: defaultSerfRetry,
			RaftUUID:  cfg.RaftUUID,
		},
		encType: cfg.EncodingFormat,
		stop:    make(chan int),
	}

	log.InitXlog(cfg.LogFile, &cfg.LogLevel)
	log.Info("Starting auth client API using gossip path: ", cfg.GossipConfigPath)

	go func() {
		if err := c.sd.StartClientAPI(c.stop, cfg.GossipConfigPath); err != nil {
			log.Fatal("Error while starting auth client API: ", err)
		}
	}()

	log.Info("Successfully initialized auth client: ", cfg.AppUUID)
	tearDown := func() {
		c.once.Do(func() {
			close(c.stop)
		})
	}
	return c, tearDown
}

// request sends a request to auth server
func (c *Client) request(data []byte, url string, isWrite bool) ([]byte, error) {
	c.sd.TillReady("PROXY", 5)
	resp, err := c.sd.Request(data, "/func?"+url, isWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to auth server: %w", err)
	}
	return resp, nil
}

// executePut sends write request with proper sequencing
func (c *Client) executePut(url string, data []byte) ([]byte, error) {
	seq := atomic.LoadUint64(&c.writeSeq)
	atomic.AddUint64(&c.writeSeq, 1)

	rncui := fmt.Sprintf("%s:0:0:0:%d", c.appUUID, seq)
	url += "&rncui=" + rncui
	return c.request(data, url, true)
}

type requestFunc func(url string, data []byte) ([]byte, error)

// doRequest performs the common logic for get/put operations
func (c *Client) doRequest(data, resp interface{}, operation string, reqFunc requestFunc) error {
	url := "name=" + operation

	encoded, err := pmCmn.Encoder(c.encType, data)
	if err != nil {
		return fmt.Errorf("failed to encode request data: %w", err)
	}

	respData, err := reqFunc(url, encoded)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	if respData == nil {
		return fmt.Errorf("received empty response from server")
	}

	if err := pmCmn.Decoder(c.encType, respData, resp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

func (c *Client) put(data, resp interface{}, operation string) error {
	return c.doRequest(data, resp, operation, c.executePut)
}

func (c *Client) get(data, resp interface{}, operation string) error {
	return c.doRequest(data, resp, operation, func(url string, data []byte) ([]byte, error) {
		return c.request(data, url, false)
	})
}

func (c *Client) CreateUser(user *auth.UserAuth) (*auth.AuthResponse, error) {
	resp := &auth.AuthResponse{}
	if err := c.put(user, resp, auth.PutSecretKeyAPI); err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}
	return resp, nil
}

// UpdateUser updates an existing user's username and/or status.
// UserID is required and user must exist.
func (c *Client) UpdateUser(user *auth.UserAuth) (*auth.AuthResponse, error) {
	if user.UserID == "" {
		return nil, fmt.Errorf("userID is required for update operation")
	}

	user.IsUpdate = true
	resp := &auth.AuthResponse{}
	if err := c.put(user, resp, auth.PutSecretKeyAPI); err != nil {
		return nil, fmt.Errorf("failed to update user: %w", err)
	}
	return resp, nil
}

func (c *Client) ListUsers(req auth.GetAuthReq) ([]auth.UserAuth, error) {
	var users []auth.UserAuth
	if err := c.get(req, &users, auth.GetSecretKeyAPI); err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}
	return users, nil
}

func (c *Client) GetUser(userID string) (*auth.UserAuth, error) {
	req := auth.GetAuthReq{UserID: userID}
	users, err := c.ListUsers(req)
	if err != nil {
		return nil, err
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("user not found: %s", userID)
	}
	return &users[0], nil
}

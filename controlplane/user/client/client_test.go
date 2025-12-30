package client

import (
	"os"
	"testing"

	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newClient(t *testing.T) (*Client, func()) {
	t.Helper()

	clusterID := os.Getenv("RAFT_ID")
	if clusterID == "" {
		log.Fatal("RAFT_ID env variable not set")
	}

	configPath := os.Getenv("GOSSIP_NODES_PATH")
	if configPath == "" {
		log.Fatal("GOSSIP_NODES_PATH env variable not set")
	}

	cfg := Config{
		AppUUID:          uuid.New().String(),
		RaftUUID:         clusterID,
		GossipConfigPath: configPath,
	}

	c, tearDown := New(cfg)
	if c == nil {
		t.Fatal("failed to initialize auth client")
	}
	return c, tearDown
}

func TestCreateUser(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]

	user := &userlib.UserReq{
		Username: username,
		// UserID is generated server-side
		Capabilities: []userlib.Capability{
			{Service: "cp", Perm: 06},
			{Service: "nisd", Perm: 06},
		},
	}

	resp, err := c.CreateUser(user)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.SecretKey)
	assert.NotEmpty(t, resp.UserID) // UUID should be generated
	assert.Equal(t, username, resp.Username)
	assert.Equal(t, userlib.StatusActive, resp.Status)

	t.Logf("Created user %s with ID %s", username, resp.UserID)
}

func TestCreateUserWithCapabilities(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]

	user := &userlib.UserReq{
		Username: username,
		Capabilities: []userlib.Capability{
			{Service: "cp", Perm: 07},
			{Service: "nisd", Perm: 06},
		},
	}

	resp, err := c.CreateUser(user)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.SecretKey)
	assert.NotEmpty(t, resp.UserID)
	assert.Equal(t, username, resp.Username)
	assert.Equal(t, userlib.StatusActive, resp.Status)
	assert.Equal(t, len(user.Capabilities), len(resp.Capabilities))

	t.Logf("Created user %s with ID %s", username, resp.UserID)
}

func TestListUsers(t *testing.T) {
	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]

	// Create a user
	user := &userlib.UserReq{
		Username: username,
	}

	resp, err := c.CreateUser(user)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// Retrieve the user by ID
	req := userlib.GetReq{UserID: resp.UserID}
	users, err := c.ListUsers(req)
	assert.NoError(t, err)
	assert.NotNil(t, users)
	require.Greater(t, len(users), 0)
	assert.Equal(t, resp.UserID, users[0].UserID)
	assert.Equal(t, username, users[0].Username)
}

func TestGetUserByID(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]

	createUser := &userlib.UserReq{
		Username: username,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)

	req := userlib.GetReq{UserID: createResp.UserID}
	users, err := c.ListUsers(req)
	assert.NoError(t, err)
	require.Len(t, users, 1)

	got := users[0]
	assert.Equal(t, createResp.UserID, got.UserID)
	assert.Equal(t, username, got.Username)

	t.Logf("Retrieved user %s with ID %s", username, createResp.UserID)
}

func TestUpdateUser(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create a user first
	originalUsername := "original_" + uuid.New().String()[:8]
	createUser := &userlib.UserReq{
		Username: originalUsername,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.UserID)

	// Update username
	newUsername := "updated_" + uuid.New().String()[:8]
	updateUser := &userlib.UserReq{
		UserID:   createResp.UserID,
		Username: newUsername,
	}

	updateResp, err := c.UpdateUser(updateUser)
	assert.NoError(t, err)
	assert.NotNil(t, updateResp)
	assert.True(t, updateResp.Success)
	assert.Equal(t, createResp.UserID, updateResp.UserID)
	assert.Equal(t, newUsername, updateResp.Username)
	assert.Empty(t, updateResp.SecretKey) // Secret key not returned on update

	t.Logf("Updated user %s: %s -> %s", createResp.UserID, originalUsername, newUsername)
}

func TestUpdateUserCapabilities(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create a user
	username := "captest_" + uuid.New().String()[:8]
	createUser := &userlib.UserReq{
		Username: username,
		Capabilities: []userlib.Capability{
			{Service: "cp", Perm: 06},
		},
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)

	// Update capabilities
	updateUser := &userlib.UserReq{
		UserID: createResp.UserID,
		Capabilities: []userlib.Capability{
			{Service: "cp", Perm: 07},
			{Service: "nisd", Perm: 06},
		},
	}

	updateResp, err := c.UpdateUser(updateUser)
	assert.NoError(t, err)
	assert.True(t, updateResp.Success)

	t.Logf("Updated user %s capabilities", createResp.UserID)
}

func TestUpdateUserNotFound(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Try to update non-existent user with a random UUID
	nonExistentID := uuid.New().String()
	updateUser := &userlib.UserReq{
		UserID:   nonExistentID,
		Username: "newname",
	}

	resp, err := c.UpdateUser(updateUser)

	// Should fail - user doesn't exist
	failed := err != nil || (resp != nil && !resp.Success)
	assert.True(t, failed, "expected update of non-existent user to fail")

	t.Logf("UpdateUser correctly failed for non-existent user %s", nonExistentID)
}

func TestGetUserReturnsDecryptedSecretKey(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "secrettest_" + uuid.New().String()[:8]

	// Create a user
	createUser := &userlib.UserReq{
		Username: username,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.SecretKey, "CreateUser should return secret key")

	// Store the original secret key from create response
	originalSecretKey := createResp.SecretKey

	// Get the user and verify secret key is returned decrypted
	getResp, err := c.GetUser(createResp.UserID)
	assert.NoError(t, err)
	assert.NotNil(t, getResp)
	assert.NotEmpty(t, getResp.SecretKey, "GetUser should return decrypted secret key")
	assert.Equal(t, originalSecretKey, getResp.SecretKey, "GetUser should return same secret key as CreateUser")

	t.Logf("Verified secret key returned correctly for user %s", createResp.UserID)
}

func TestGetUser(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	users, err := c.ListUsers(userlib.GetReq{})
	assert.NoError(t, err, "error not expect")

	t.Logf("Verified user get call, got: %v users", len(users))
}

func TestCreateAdminUser(t *testing.T) {
	c, tearDown := newClient(t)
	defer tearDown()

	// Create admin user with custom parameters
	req := &userlib.UserReq{
		Username:     userlib.AdminUsername,
		NewSecretKey: "my-custom-admin-secret",
	}

	resp, err := c.CreateAdminUser(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.Equal(t, userlib.AdminUsername, resp.Username)
	assert.Equal(t, uuid.Nil.String(), resp.UserID)
	assert.NotEmpty(t, resp.SecretKey)

	t.Logf("Admin user created/exists with ID: %s", resp.UserID)

	// Call again - should be idempotent (returns existing admin)
	resp2, err := c.CreateAdminUser(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp2)
	assert.True(t, resp2.Success)
	assert.Equal(t, resp.UserID, resp2.UserID)

	t.Logf("CreateAdminUser is idempotent - second call succeeded")
}

func TestUpdateAdminSecretKey(t *testing.T) {
	c, tearDown := newClient(t)
	defer tearDown()

	// First ensure admin user exists
	adminReq := &userlib.UserReq{
		Username:     userlib.AdminUsername,
		NewSecretKey: "initial-secret-key",
	}
	adminResp, err := c.CreateAdminUser(adminReq)
	assert.NoError(t, err)
	assert.NotNil(t, adminResp)
	assert.True(t, adminResp.Success)

	originalSecretKey := adminResp.SecretKey
	t.Logf("Original admin secret key: %s", originalSecretKey)

	// Update the admin secret key
	newSecretKey := "my-new-custom-secret-key-12345"
	updateResp, err := c.UpdateAdminSecretKey(adminResp.UserID, newSecretKey)
	assert.NoError(t, err)
	assert.NotNil(t, updateResp)
	assert.True(t, updateResp.Success)
	assert.Equal(t, newSecretKey, updateResp.SecretKey)

	t.Logf("Admin secret key updated successfully")

	// Verify by getting the user
	getResp, err := c.GetUser(adminResp.UserID)
	assert.NoError(t, err)
	assert.NotNil(t, getResp)
	assert.Equal(t, newSecretKey, getResp.SecretKey)

	t.Logf("Verified admin secret key was updated correctly")
}

func TestUpdateSecretKeyNonAdminFails(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create a regular (non-admin) user
	username := "regularuser_" + uuid.New().String()[:8]
	createResp, err := c.CreateUser(&userlib.UserReq{
		Username: username,
	})
	assert.NoError(t, err)
	assert.True(t, createResp.Success)

	// Try to update secret key for non-admin user - should fail
	updateResp, err := c.UpdateAdminSecretKey(createResp.UserID, "new-secret")

	// Should fail because user is not admin
	failed := err != nil || (updateResp != nil && !updateResp.Success)
	assert.True(t, failed, "expected secret key update to fail for non-admin user")

	t.Logf("UpdateAdminSecretKey correctly rejected non-admin user")
}

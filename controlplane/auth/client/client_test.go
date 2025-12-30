package client

import (
	"os"
	"testing"

	auth "github.com/00pauln00/niova-mdsvc/controlplane/auth/lib"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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

	user := &auth.UserAuth{
		Username: username,
		Status:   auth.StatusActive,
	}

	resp, err := c.CreateUser(user)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.SecretKey)
	assert.NotEmpty(t, resp.UserID)
	assert.Equal(t, username, resp.Username)
	assert.Equal(t, auth.StatusActive, resp.Status)

	t.Logf("Created user %s with ID %s", username, resp.UserID)
}

func TestCreateUserWithCustomID(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]
	userID := uuid.New().String()

	user := &auth.UserAuth{
		Username: username,
		UserID:   userID,
		Status:   auth.StatusActive,
	}

	resp, err := c.CreateUser(user)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.SecretKey)
	assert.Equal(t, userID, resp.UserID)
	assert.Equal(t, username, resp.Username)
	assert.Equal(t, auth.StatusActive, resp.Status)

	t.Logf("Created user %s with custom ID %s", username, userID)
}

func TestListUsers(t *testing.T) {
	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]

	// Create a user
	user := &auth.UserAuth{
		Username: username,
		Status:   auth.StatusActive,
	}

	resp, err := c.CreateUser(user)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// Retrieve the user by ID
	req := auth.GetAuthReq{UserID: resp.UserID}
	users, err := c.ListUsers(req)
	assert.NoError(t, err)
	assert.NotNil(t, users)
	assert.Greater(t, len(users), 0)
	assert.Equal(t, resp.UserID, users[0].UserID)
	assert.Equal(t, username, users[0].Username)
}

func TestGetUserByID(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]

	createUser := &auth.UserAuth{
		Username: username,
		Status:   auth.StatusActive,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)

	req := auth.GetAuthReq{UserID: createResp.UserID}
	users, err := c.ListUsers(req)
	assert.NoError(t, err)
	assert.Len(t, users, 1)

	got := users[0]
	assert.Equal(t, createResp.UserID, got.UserID)
	assert.Equal(t, username, got.Username)
	assert.Equal(t, auth.StatusActive, got.Status)

	t.Logf("Retrieved user %s with ID %s", username, createResp.UserID)
}

func TestCreateUserDuplicateUsername(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "testdup_" + uuid.New().String()[:8]

	// First creation - should succeed
	user1 := &auth.UserAuth{
		Username: username,
		Status:   auth.StatusActive,
	}
	resp1, err := c.CreateUser(user1)
	assert.NoError(t, err)
	assert.True(t, resp1.Success)

	// Second creation with same username - should fail
	user2 := &auth.UserAuth{
		Username: username,
		Status:   auth.StatusActive,
	}
	resp2, err2 := c.CreateUser(user2)

	// Handles both possible failure modes: err != nil or Success == false
	failed := err2 != nil || (resp2 != nil && !resp2.Success)
	assert.True(t, failed, "expected duplicate username to fail")
}

func TestUpdateUser(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create a user first
	originalUsername := "original_" + uuid.New().String()[:8]
	createUser := &auth.UserAuth{
		Username: originalUsername,
		Status:   auth.StatusActive,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.UserID)

	// Update username
	newUsername := "updated_" + uuid.New().String()[:8]
	updateUser := &auth.UserAuth{
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

func TestUpdateUserStatus(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create a user
	username := "statustest_" + uuid.New().String()[:8]
	createUser := &auth.UserAuth{
		Username: username,
		Status:   auth.StatusActive,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)

	// Update status
	updateUser := &auth.UserAuth{
		UserID: createResp.UserID,
		Status: auth.StatusInactive,
	}

	updateResp, err := c.UpdateUser(updateUser)
	assert.NoError(t, err)
	assert.True(t, updateResp.Success)
	assert.Equal(t, auth.StatusInactive, updateResp.Status)

	t.Logf("Updated user %s status: active -> inactive", createResp.UserID)
}

func TestCreateUserDuplicateCustomID(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	userID := uuid.New().String()
	username1 := "testid1_" + uuid.New().String()[:8]
	username2 := "testid2_" + uuid.New().String()[:8]

	// First creation - should succeed
	user1 := &auth.UserAuth{
		Username: username1,
		UserID:   userID,
		Status:   auth.StatusActive,
	}
	resp1, err := c.CreateUser(user1)
	assert.NoError(t, err)
	assert.True(t, resp1.Success)
	assert.Equal(t, userID, resp1.UserID)

	// Second creation with same custom ID - should fail
	user2 := &auth.UserAuth{
		Username: username2,
		UserID:   userID,
		Status:   auth.StatusActive,
	}
	resp2, err2 := c.CreateUser(user2)

	failed := err2 != nil || (resp2 != nil && !resp2.Success)
	assert.True(t, failed, "expected duplicate custom UserID to fail")
}

func TestUpdateUserDuplicateUsername(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create two users
	username1 := "user1_" + uuid.New().String()[:8]
	username2 := "user2_" + uuid.New().String()[:8]

	user1 := &auth.UserAuth{Username: username1, Status: auth.StatusActive}
	user2 := &auth.UserAuth{Username: username2, Status: auth.StatusActive}

	resp1, err := c.CreateUser(user1)
	assert.NoError(t, err)
	assert.True(t, resp1.Success)

	resp2, err := c.CreateUser(user2)
	assert.NoError(t, err)
	assert.True(t, resp2.Success)

	// Try to update user2 to have user1's username - should fail
	updateUser := &auth.UserAuth{
		UserID:   resp2.UserID,
		Username: username1, // Duplicate!
	}

	updateResp, err := c.UpdateUser(updateUser)
	failed := err != nil || (updateResp != nil && !updateResp.Success)
	assert.True(t, failed, "expected update to duplicate username to fail")

	t.Log("UpdateUser correctly rejected duplicate username")
}

func TestUpdateUserNotFound(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Try to update non-existent user
	nonExistentID := uuid.New().String()
	updateUser := &auth.UserAuth{
		UserID:   nonExistentID,
		Username: "newname",
		Status:   auth.StatusActive,
	}

	resp, err := c.UpdateUser(updateUser)

	// Should fail - user doesn't exist
	failed := err != nil || (resp != nil && !resp.Success)
	assert.True(t, failed, "expected update of non-existent user to fail")

	t.Logf("UpdateUser correctly failed for non-existent user %s", nonExistentID)
}

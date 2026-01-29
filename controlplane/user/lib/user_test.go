package userlib

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserReq_Validate(t *testing.T) {
	tests := []struct {
		name      string
		userAuth  UserReq
		wantError bool
		errorMsg  string
	}{
		{
			name: "ValidUser",
			userAuth: UserReq{
				Username: "testuser",
				UserID:   uuid.New().String(),
				Capabilities: []Capability{
					{Service: "cp", Perm: 06},
				},
			},
		},
		{
			name: "ValidUserWithoutCapabilities",
			userAuth: UserReq{
				Username: "testuser",
				UserID:   uuid.New().String(),
			},
		},
		{
			name: "EmptyUsername",
			userAuth: UserReq{
				Username: "",
				UserID:   uuid.New().String(),
			},
			wantError: true,
			errorMsg:  "username cannot be empty",
		},
		{
			name: "WhitespaceUsername",
			userAuth: UserReq{
				Username: "   ",
				UserID:   uuid.New().String(),
			},
			wantError: true,
			errorMsg:  "username cannot be empty",
		},
		{
			name: "ValidUserWithMultipleCapabilities",
			userAuth: UserReq{
				Username: "testuser",
				UserID:   uuid.New().String(),
				Capabilities: []Capability{
					{Service: "cp", Perm: 07},
					{Service: "nisd", Perm: 06},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.userAuth.Validate()
			if tt.wantError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUser_Init(t *testing.T) {
	t.Run("InitializesFromUserReq", func(t *testing.T) {
		userID := uuid.New().String()
		req := &UserReq{
			Username: "testuser",
			UserID:   userID,
			Capabilities: []Capability{
				{Service: "cp", Perm: 06},
				{Service: "nisd", Perm: 06},
			},
		}

		user := &User{}
		err := user.Init(req)
		require.NoError(t, err)

		assert.Equal(t, userID, user.UserID)
		assert.Equal(t, "testuser", user.Username)
		assert.Equal(t, StatusActive, user.Status)
		assert.Len(t, user.Capabilities, 2)
	})

	t.Run("SetsDefaultStatusToActive", func(t *testing.T) {
		req := &UserReq{
			Username: "testuser",
			UserID:   uuid.New().String(),
		}

		user := &User{}
		err := user.Init(req)
		require.NoError(t, err)

		assert.Equal(t, StatusActive, user.Status)
	})
}

func TestCapability(t *testing.T) {
	t.Run("CapabilityStruct", func(t *testing.T) {
		cap := Capability{
			Service: "cp",
			Perm:    07,
		}

		assert.Equal(t, "cp", cap.Service)
		assert.Equal(t, uint32(07), cap.Perm)
	})

	t.Run("NisdCapability", func(t *testing.T) {
		cap := Capability{
			Service: "nisd",
			Perm:    06,
		}

		assert.Equal(t, "nisd", cap.Service)
		assert.Equal(t, uint32(06), cap.Perm)
	})
}

func TestStatus(t *testing.T) {
	t.Run("StatusString", func(t *testing.T) {
		assert.Equal(t, "active", StatusActive.String())
		assert.Equal(t, "inactive", StatusInactive.String())
		assert.Equal(t, "suspended", StatusSuspended.String())
		assert.Equal(t, "unknown", Status(99).String())
	})
}

func TestUser_FullWorkflow(t *testing.T) {
	t.Run("CreateAndInitUser", func(t *testing.T) {
		// Create a new user request
		userID := uuid.New().String()
		req := &UserReq{
			Username: "john",
			UserID:   userID,
			Capabilities: []Capability{
				{Service: "cp", Perm: 07},
				{Service: "nisd", Perm: 06},
			},
		}

		// Validate request
		err := req.Validate()
		require.NoError(t, err)

		// Initialize user
		user := &User{}
		err = user.Init(req)
		require.NoError(t, err)

		// Verify all fields are properly set
		assert.Equal(t, userID, user.UserID)
		assert.Equal(t, "john", user.Username)
		assert.Equal(t, StatusActive, user.Status)
		assert.Len(t, user.Capabilities, 2)

		// Verify capabilities
		assert.Equal(t, "cp", user.Capabilities[0].Service)
		assert.Equal(t, uint32(07), user.Capabilities[0].Perm)
		assert.Equal(t, "nisd", user.Capabilities[1].Service)
		assert.Equal(t, uint32(06), user.Capabilities[1].Perm)
	})
}

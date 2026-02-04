package userserver

import (
	"strings"

	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"
)

// Key schema index constants for parsing /u/<UserId>/<field> pattern
const (
	baseKeyIndex  = iota // "u"
	userIDIndex          // "<UserId>"
	fieldKeyIndex        // "<field>"
	minFieldParts = 3    // minimum parts for a valid key: u/<id>/<field>
)

// Field name constants
const (
	secretKeyField = "sk" // secret key
	usernameField  = "un" // username
	statusField    = "st" // status
	userTypeField  = "ut" // usertype (admin/non-admin)
	userRoleField  = "ur" // user role
)

// Key prefix constants
const (
	userKeyPrefix     = "u"     // /u/<UserId>/...
	userIndexPrefix   = "u-idx" // /u-idx/...
	usernameIdxPrefix = "un"    // /u-idx/un/<username> -> <UserId>
)

// Entity is an interface for all parsed entities
type Entity interface{}

// EntityDeserializer defines the interface for deserializing entities from RocksDB
type EntityDeserializer interface {
	GetRootKey() string
	NewEntity(id string) Entity
	ParseField(entity Entity, parts []string, value []byte)
	GetEntity(entity Entity) Entity
}

// UserDeserializer implements the EntityDeserializer interface for User
type UserDeserializer struct{}

func (UserDeserializer) GetRootKey() string {
	return userKeyPrefix
}

func (UserDeserializer) NewEntity(id string) Entity {
	return &userlib.User{UserID: id}
}

func (UserDeserializer) ParseField(entity Entity, parts []string, value []byte) {
	user := entity.(*userlib.User)

	if len(parts) < minFieldParts {
		return
	}

	field := parts[fieldKeyIndex]
	switch field {
	case secretKeyField:
		user.SecretKey = string(value)
	case usernameField:
		user.Username = string(value)
	case statusField:
		if len(value) > 0 {
			user.Status = userlib.Status(value[0])
		}
	case userTypeField:
		user.IsAdmin = string(value) == userlib.AdminUsername
	case userRoleField:
		user.UserRole = string(value)
	}
}

func (UserDeserializer) GetEntity(entity Entity) Entity {
	return *entity.(*userlib.User)
}

// DeserializeUsers deserializes User entities from RocksDB read result
func DeserializeUsers(readResult map[string][]byte) []userlib.User {
	entityMap := make(map[string]Entity)
	deserializer := UserDeserializer{}

	for k, v := range readResult {
		parts := strings.Split(strings.Trim(k, "/"), "/")

		// Skip if not enough parts or wrong prefix
		if len(parts) < minFieldParts {
			continue
		}

		// Check root key prefix
		if parts[baseKeyIndex] != deserializer.GetRootKey() {
			continue
		}

		// UserID is now a UUID string
		userID := parts[userIDIndex]
		if userID == "" { // Skip malformed keys with empty UserID
			continue
		}

		entity, exists := entityMap[userID]
		if !exists {
			entity = deserializer.NewEntity(userID)
			entityMap[userID] = entity
		}
		deserializer.ParseField(entity, parts, v)
	}

	result := make([]userlib.User, 0, len(entityMap))
	for _, e := range entityMap {
		final := deserializer.GetEntity(e)
		result = append(result, final.(userlib.User))
	}

	return result
}

// DeserializeSingleUser deserializes a single user from RocksDB read result
func DeserializeSingleUser(readResult map[string][]byte, userID string) *userlib.User {
	users := DeserializeUsers(readResult)
	for i := range users {
		if users[i].UserID == userID {
			return &users[i]
		}
	}
	return nil
}

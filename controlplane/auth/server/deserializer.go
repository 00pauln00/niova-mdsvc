package authserver

import (
	"strings"
	"time"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
	auth "github.com/00pauln00/niova-mdsvc/controlplane/auth/lib"
)

const (
	baseKeyIndex = iota
	usersPrefixIndex
	userIDIndex
	fieldKeyIndex
	minFieldKeyParts
)

const (
	secretKeyField = "sk" // secret key
	usernameField  = "un" // username
	statusField    = "st" // status
	createdAtField = "ca" // createdAt
	updatedAtField = "ua" // updatedAt
)

const (
	authKey           = "auth"
	authUsersKey      = "users"
	authUsernamesKey  = "usernames"
	authSecretKeysKey = "secretkeys"
)

// Entity is an interface for all parsed entites
type Entity interface{}

// EntityDeserializer defines the interface for deserializing entities from RocksDB
type EntityDeserializer interface {
	GetRootKey() string
	NewEntity(id string) Entity
	ParseField(entity Entity, parts []string, value []byte)
	GetEntity(entity Entity) Entity
}

// UserAuthDeserializer implements the EntityDeserializer interface for UserAuth
type UserAuthDeserializer struct{}

func (UserAuthDeserializer) GetRootKey() string {
	return authKey
}

func (UserAuthDeserializer) NewEntity(id string) Entity {
	return &auth.UserAuth{UserID: id}
}

func (UserAuthDeserializer) ParseField(entity Entity, parts []string, value []byte) {
	user := entity.(*auth.UserAuth)

	if len(parts) >= minFieldKeyParts {
		field := parts[fieldKeyIndex]
		switch field {
		case secretKeyField:
			user.SecretKey = string(value)
		case usernameField:
			user.Username = string(value)
		case statusField:
			// Parse status from single byte (uint8)
			if len(value) > 0 {
				status := auth.Status(value[0])
				user.Status = status
			}
		case createdAtField:
			r, err := time.Parse(time.RFC3339, string(value))
			if err != nil {
				log.Errorf("Failed to parse createdAt: %v", err)
			} else {
				user.CreatedAt = r
			}
		case updatedAtField:
			r, err := time.Parse(time.RFC3339, string(value))
			if err != nil {
				log.Errorf("Failed to parse updatedAt: %v", err)
			} else {
				user.UpdatedAt = r
			}
		}
	}
}

func (UserAuthDeserializer) GetEntity(entity Entity) Entity {
	return *entity.(*auth.UserAuth)
}

func DeserializeEntities[T Entity](readResult map[string][]byte, deserializer EntityDeserializer) []T {
	entityMap := make(map[string]Entity)

	for k, v := range readResult {
		parts := strings.Split(strings.Trim(k, "/"), "/")

		// Valid key must have at lease the user ID part
		// eg. auth/users/{userID}
		if len(parts) <= userIDIndex {
			continue
		}

		// check for expected key prefixes
		if parts[baseKeyIndex] != deserializer.GetRootKey() || parts[usersPrefixIndex] != authUsersKey {
			continue
		}

		id := parts[userIDIndex]
		entity, exists := entityMap[id]
		if !exists {
			entity = deserializer.NewEntity(id)
			entityMap[id] = entity
		}
		deserializer.ParseField(entity, parts, v)
	}

	result := make([]T, 0, len(entityMap))
	for _, e := range entityMap {
		final := deserializer.GetEntity(e)
		result = append(result, final.(T))
	}

	return result
}

func DeserializeSingleEntity(readResult map[string][]byte, userID string) *auth.UserAuth {
	deserializer := UserAuthDeserializer{}
	entities := DeserializeEntities[auth.UserAuth](readResult, deserializer)

	if len(entities) > 0 {
		return &entities[0]
	}

	return nil
}

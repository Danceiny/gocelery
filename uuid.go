package gocelery

import "github.com/satori/go.uuid"

// generateUUID generates a v4 uuid and returns it as a string
func generateUUID() string {
    return uuid.NewV4().String()
}

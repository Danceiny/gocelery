package gocelery

import "github.com/Danceiny/go.uuid"

// generateUUID generates a v4 uuid and returns it as a string
func generateUUID() string {
    uuid4, _ := uuid.NewV4()
    return uuid4.String()
}

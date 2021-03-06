package gocelery

import (
    "fmt"

    "github.com/garyburd/redigo/redis"
)

// RedisCeleryBackend is CeleryBackend for Redis
type RedisCeleryBackend struct {
    *redis.Pool
}

// Support Broker Options: https://github.com/gocelery/gocelery/pull/31
func NewRedisCeleryBackend(host string, port int, db int, pass string) *RedisCeleryBackend {
    return &RedisCeleryBackend{
        Pool: NewRedisPool(host, port, db, pass),
    }
}

// GetResult calls API to get asynchronous result
// Should be called by AsyncResult
func (cb *RedisCeleryBackend) GetResult(taskID string) (*ResultMessage, error) {
    // "celery-task-meta-" + taskID
    conn := cb.Get()
    defer conn.Close()
    val, err := conn.Do("GET", fmt.Sprintf("celery-task-meta-%s", taskID))
    if err != nil {
        return nil, err
    }
    if val == nil {
        return nil, fmt.Errorf("result not available")
    }
    var resultMessage ResultMessage
    err = json.Unmarshal(val.([]byte), &resultMessage)
    if err != nil {
        return nil, err
    }
    return &resultMessage, nil
}

// SetResult pushes result back into backend
func (cb *RedisCeleryBackend) SetResult(taskID string, result *ResultMessage) error {
    resBytes, err := json.Marshal(result)
    if err != nil {
        return err
    }
    conn := cb.Get()
    defer conn.Close()
    _, err = conn.Do("SETEX", fmt.Sprintf("celery-task-meta-%s", taskID), 86400, resBytes)
    return err
}

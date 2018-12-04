package main

import (
    "fmt"
    "time"

    "github.com/Danceiny/gocelery"
)

// Celery Itf_CeleryTask using args
func add(a int, b int) int {
    return a + b
}

// AddTask is Celery Itf_CeleryTask using kwargs
type AddTask struct {
    a int // x
    b int // y
}

// ParseKwargs parses kwargs
func (a *AddTask) ParseKwargs(kwargs map[string]interface{}) error {
    kwargA, ok := kwargs["x"]
    if !ok {
        return fmt.Errorf("undefined kwarg x")
    }
    kwargAFloat, ok := kwargA.(float64)
    if !ok {
        return fmt.Errorf("malformed kwarg x")
    }
    a.a = int(kwargAFloat)
    kwargB, ok := kwargs["y"]
    if !ok {
        return fmt.Errorf("undefined kwarg y")
    }
    kwargBFloat, ok := kwargB.(float64)
    if !ok {
        return fmt.Errorf("malformed kwarg y")
    }
    a.b = int(kwargBFloat)
    return nil
}

// RunTask executes actual task
func (a *AddTask) RunTask() (interface{}, error) {
    result := a.a + a.b
    return result, nil
}

func main() {
    // create broker and backend
    celeryBroker := gocelery.NewRedisCeleryBroker("localhost", 6379, 0, "")
    celeryBackend := gocelery.NewRedisCeleryBackend("localhost", 6379, 0, "")

    // AMQP example
    // celeryBroker := gocelery.NewAMQPCeleryBroker("amqp://")
    // celeryBackend := gocelery.NewAMQPCeleryBackend("amqp://")

    // Configure with 2 celery workers
    celeryServer, _ := gocelery.NewCeleryServer(celeryBroker, celeryBackend, 2)

    // worker.add name reflects "add" task method found in "worker.py"
    // this worker uses args
    celeryServer.Register("worker.add", add)
    celeryServer.Register("worker.add_reflect", &AddTask{})

    // Start Worker - blocking method
    go celeryServer.StartWorker()
    // Wait 30 seconds and stop all workers
    time.Sleep(60 * time.Second)
    celeryServer.StopWorker()
}

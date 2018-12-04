package main

import (
    "bytes"
    "fmt"
    "log"
    "math/rand"
    "os/exec"
    "reflect"
    "time"

    "github.com/Danceiny/gocelery"
)

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle

func runCeleryWorker(pyEnv string) {
    activatePyEnvCmd := ""
    if pyEnv != "" {
        activatePyEnvCmd = "source activate " + pyEnv + " &&"
    }
    cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("%s celery -A worker worker --loglevel=debug --without-heartbeat "+
        "--without-mingle", activatePyEnvCmd))
    var out bytes.Buffer
    cmd.Stdout = &out
    err := cmd.Run()
    if err != nil {
        log.Printf("run celery worker failed: %s", err.Error())
    }
    return
}

func runRedisInDocker(password string) {
    var requirePass = "";
    if password != "" {
        requirePass = "--requirepass " + password;
    }
    err := exec.Command("/bin/bash", "-c", fmt.Sprintf(
        "docker run --name redis-6379 -p 6379:6379 -d redis %s", requirePass)).Run()
    if err != nil {
        log.Printf("run redis server failed: %s", err.Error())
    }
}
func main() {
    var redisPassword = ""
    var redisHost = "localhost"
    var redisPort = 6379
    // // 启动redis
    // runRedisInDocker(redisPassword)
    // // 启动celery worker (python)
    // runCeleryWorker("py27")

    // create broker and backend
    celeryBroker := gocelery.NewRedisCeleryBroker(redisHost, redisPort, 0, redisPassword)
    celeryBackend := gocelery.NewRedisCeleryBackend(redisHost, redisPort, 0, redisPassword)

    // AMQP example
    // celeryBroker := gocelery.NewAMQPCeleryBroker("amqp://")
    // celeryBackend := gocelery.NewAMQPCeleryBackend("amqp://")

    // create client
    celeryClient, _ := gocelery.NewCeleryClient(celeryBroker, celeryBackend)

    arg1 := rand.Intn(10)
    arg2 := rand.Intn(110)

    asyncResult, err := celeryClient.Delay("worker.add", arg1, arg2)
    if err != nil {
        log.Fatalf("%s", err.Error())
        panic(err)
    }
    log.Printf("task id of async result: %s", asyncResult.GetTaskId())
    res, err := asyncResult.Get(10 * time.Second)
    if err != nil {
        fmt.Println(err)
    } else {
        fmt.Printf("Result: %v of type: %v\n", res, reflect.TypeOf(res))
    }

    // test more detailed apply
    expireTime := time.Now().Add(100 * time.Second);
    asyncResult, err = celeryClient.ApplyAsync("worker.add", []interface{}{arg1, arg2},
        nil, &expireTime, nil, true, "testqueue", 10, "testroutingkey", "")
    if err != nil {
        panic(err)
    }
    res, err = asyncResult.Get(10 * time.Second)
    if err != nil {
        fmt.Println(err)
    } else {
        fmt.Printf("Result: %v of type: %v\n", res, reflect.TypeOf(res))
    }

    // send task
    /*
        asyncResult, err = celeryClient.DelayKwargs("worker.add_reflect", map[string]interface{}{
            "x": 3,
            "y": 5,
        })
        if err != nil {
            panic(err)
        }

        // check if result is ready
        isReady, _ := asyncResult.Ready()
        fmt.Printf("Ready status: %v\n", isReady)

        // get result with 1s timeout
        res2, err := asyncResult.Get(10 * time.Second)
        if err != nil {
            fmt.Println(err)
        } else {
            fmt.Printf("Result: %v of type: %v\n", res2, reflect.TypeOf(res2))
        }
    */
}

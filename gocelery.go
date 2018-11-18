package gocelery

import (
    "fmt"
    "time"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
    broker  CeleryBroker
    backend CeleryBackend
    worker  *CeleryWorker
}

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
    SendCeleryMessage(*CeleryMessage) error
    GetTaskMessage() (*CeleryTask, error) // must be non-blocking
}

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
    GetResult(string) (*ResultMessage, error) // must be non-blocking
    SetResult(taskID string, result *ResultMessage) error
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend, numWorkers int) (*CeleryClient, error) {
    return &CeleryClient{
        broker,
        backend,
        NewCeleryWorker(broker, backend, numWorkers),
    }, nil
}

// Register task
func (cc *CeleryClient) Register(name string, task interface{}) {
    cc.worker.Register(name, task)
}

// StartWorker starts celery workers
func (cc *CeleryClient) StartWorker() {
    cc.worker.StartWorker()
}

// StopWorker stops celery workers
func (cc *CeleryClient) StopWorker() {
    cc.worker.StopWorker()
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(task string, args ...interface{}) (*AsyncResult, error) {
    celeryTask := getTaskObj(task)
    celeryTask.Args = args
    return cc.delay(celeryTask, nil)
}

// DelayKwargs gets asynchronous results with argument map
func (cc *CeleryClient) DelayKwargs(task string, args map[string]interface{}) (*AsyncResult, error) {
    celeryTask := getTaskObj(task)
    celeryTask.Kwargs = args
    return cc.delay(celeryTask, nil)
}
func (cc *CeleryClient) ApplyAsync(task string, args []interface{}, kwargs map[string]interface{},
    expires *time.Time, eta *time.Time, retry bool, queue string,
    priority int, routingKey string, exchange string) (*AsyncResult, error) {
    celeryTask := getTaskObj(task)
    if kwargs != nil {
        celeryTask.Kwargs = kwargs
    }
    if args != nil {
        celeryTask.Args = args
    }
    if eta != nil {
        celeryTask.ETA = *eta
    }
    if expires != nil {
        celeryTask.Expires = *expires
    }
    celeryTask.Priority = priority
    return cc.delay(celeryTask, NewCeleryDeliveryInfo(routingKey, exchange))

    /*

    def apply_async(self, args=None, kwargs=None, task_id=None, producer=None,
                    link=None, link_error=None, shadow=None, **options):

    """Apply tasks asynchronously by sending a message.

    Arguments:
        args (Tuple): The positional arguments to pass on to the task.

        kwargs (Dict): The keyword arguments to pass on to the task.

        countdown (float): Number of seconds into the future that the
            task should execute.  Defaults to immediate execution.

        eta (~datetime.datetime): Absolute time and date of when the task
            should be executed.  May not be specified if `countdown`
            is also supplied.

        expires (float, ~datetime.datetime): Datetime or
            seconds in the future for the task should expire.
            The task won't be executed after the expiration time.

        shadow (str): Override task name used in logs/monitoring.
            Default is retrieved from :meth:`shadow_name`.

        connection (kombu.Connection): Re-use existing broker connection
            instead of acquiring one from the connection pool.

        retry (bool): If enabled sending of the task message will be
            retried in the event of connection loss or failure.
            Default is taken from the :setting:`task_publish_retry`
            setting.  Note that you need to handle the
            producer/connection manually for this to work.

        retry_policy (Mapping): Override the retry policy used.
            See the :setting:`task_publish_retry_policy` setting.

        queue (str, kombu.Queue): The queue to route the task to.
            This must be a key present in :setting:`task_queues`, or
            :setting:`task_create_missing_queues` must be
            enabled.  See :ref:`guide-routing` for more
            information.

        exchange (str, kombu.Exchange): Named custom exchange to send the
            task to.  Usually not used in combination with the ``queue``
            argument.

        routing_key (str): Custom routing key used to route the task to a
            worker server.  If in combination with a ``queue`` argument
            only used to specify custom routing keys to topic exchanges.

        priority (int): The task priority, a number between 0 and 9.
            Defaults to the :attr:`priority` attribute.

        serializer (str): Serialization method to use.
            Can be `pickle`, `json`, `yaml`, `msgpack` or any custom
            serialization method that's been registered
            with :mod:`kombu.serialization.registry`.
            Defaults to the :attr:`serializer` attribute.

        compression (str): Optional compression method
            to use.  Can be one of ``zlib``, ``bzip2``,
            or any custom compression methods registered with
            :func:`kombu.compression.register`.
            Defaults to the :setting:`task_compression` setting.

        link (~@Signature): A single, or a list of tasks signatures
            to apply if the task returns successfully.

        link_error (~@Signature): A single, or a list of task signatures
            to apply if an error occurs while executing the task.

        producer (kombu.Producer): custom producer to use when publishing
            the task.

        add_to_parent (bool): If set to True (default) and the task
            is applied while executing another task, then the result
            will be appended to the parent tasks ``request.children``
            attribute.  Trailing can also be disabled by default using the
            :attr:`trail` attribute

        publisher (kombu.Producer): Deprecated alias to ``producer``.

        headers (Dict): Message headers to be included in the message.
     */

}
func (cc *CeleryClient) delay(task *CeleryTask, info *CeleryDeliveryInfo) (*AsyncResult, error) {
    defer releaseTaskMessage(task)
    celeryMessage := loadCeleryMessage(task);
    defer releaseCeleryMessage(celeryMessage)
    err := cc.broker.SendCeleryMessage(celeryMessage)
    if err != nil {
        return nil, err
    }
    return &AsyncResult{
        taskID:  task.Id,
        backend: cc.backend,
    }, nil
}

// Itf_CeleryTask is an interface that represents actual task
// Passing Itf_CeleryTask interface instead of function pointer
// avoids reflection and may have performance gain.
// ResultMessage must be obtained using GetResultMessage()
type Itf_CeleryTask interface {
    // ParseKwargs - define a method to parse kwargs
    ParseKwargs(map[string]interface{}) error

    // RunTask - define a method to run
    RunTask() (interface{}, error)
}

// AsyncResult is pending result
type AsyncResult struct {
    taskID  string
    backend CeleryBackend
    result  *ResultMessage
}

func (ar *AsyncResult) GetTaskId() string {
    return ar.taskID
}

// Get gets actual result from redis
// It blocks for period of time set by timeout and return error if unavailable
func (ar *AsyncResult) Get(timeout time.Duration) (interface{}, error) {
    ticker := time.NewTicker(50 * time.Millisecond)
    timeoutChan := time.After(timeout)
    for {
        select {
        case <-timeoutChan:
            err := fmt.Errorf("%v timeout getting result for %s", timeout, ar.taskID)
            return nil, err
        case <-ticker.C:
            val, err := ar.AsyncGet()
            if err != nil {
                continue
            }
            return val, nil
        }
    }
}

// AsyncGet gets actual result from redis and returns nil if not available
func (ar *AsyncResult) AsyncGet() (interface{}, error) {
    if ar.result != nil {
        return ar.result.Result, nil
    }
    // process
    val, err := ar.backend.GetResult(ar.taskID)
    if err != nil {
        return nil, err
    }
    if val == nil {
        return nil, err
    }
    if val.Status != "SUCCESS" {
        return nil, fmt.Errorf("error response status %v", val)
    }
    ar.result = val
    return val.Result, nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready() (bool, error) {
    if ar.result != nil {
        return true, nil
    }
    val, err := ar.backend.GetResult(ar.taskID)
    if err != nil {
        return false, err
    }
    ar.result = val
    return (val != nil), nil
}

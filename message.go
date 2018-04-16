package gocelery

import (
    "encoding/base64"
    "encoding/json"
    "log"
    "reflect"
    "sync"
    "time"
    "github.com/astaxie/beego/logs"
)

// CeleryMessage is actual message to be sent to Redis
type CeleryMessage struct {
    Body            string                 `json:"body"`
    Headers         map[string]interface{} `json:"headers"`
    ContentType     string                 `json:"content-type"`
    Properties      CeleryProperties       `json:"properties"`
    ContentEncoding string                 `json:"content-encoding"`
}

func (cm *CeleryMessage) reset() {
    cm.Headers = nil
    cm.Body = ""
    cm.Properties.CorrelationID = generateUUID()
    cm.Properties.ReplyTo = generateUUID()
    cm.Properties.DeliveryTag = generateUUID()
}

//TODO: support customized delivery_info property
var celeryMessagePool = sync.Pool{
    New: func() interface{} {
        return &CeleryMessage{
            Body:        "",
            Headers:     nil,
            ContentType: "application/json",
            Properties: CeleryProperties{
                BodyEncoding:  "base64",
                CorrelationID: generateUUID(),
                ReplyTo:       generateUUID(),
                //DeliveryInfo:  *getDefaultCeleryDeliveryInfo(),
                DeliveryMode: 2,
                DeliveryTag:  generateUUID(),
            },
            ContentEncoding: "utf-8",
        }
    },
}

func getDefaultCeleryDeliveryInfo() *CeleryDeliveryInfo {
    return &CeleryDeliveryInfo{
        Priority:   0,
        RoutingKey: "celery",
        Exchange:   "celery",
    }
}
func NewCeleryDeliveryInfo(priority int, routingKey string, exchange string) *CeleryDeliveryInfo {
    return &CeleryDeliveryInfo{
        Priority:   priority,
        RoutingKey: routingKey,
        Exchange:   exchange,
    }
}
func getCeleryMessage(encodedTaskMessage string, deliveryInfo *CeleryDeliveryInfo) *CeleryMessage {
    msg := celeryMessagePool.Get().(*CeleryMessage)
    msg.Body = encodedTaskMessage
    if deliveryInfo == nil {
        deliveryInfo = getDefaultCeleryDeliveryInfo()
    }
    msg.Properties.DeliveryInfo = *deliveryInfo
    return msg
}

func releaseCeleryMessage(v *CeleryMessage) {
    v.reset()
    celeryMessagePool.Put(v)
}

// CeleryProperties represents properties json
type CeleryProperties struct {
    BodyEncoding  string             `json:"body_encoding"`
    CorrelationID string             `json:"correlation_id"`
    ReplyTo       string             `json:"replay_to"`
    DeliveryInfo  CeleryDeliveryInfo `json:"delivery_info"`
    DeliveryMode  int                `json:"delivery_mode"`
    DeliveryTag   string             `json:"delivery_tag"`
}

// CeleryDeliveryInfo represents deliveryinfo json
type CeleryDeliveryInfo struct {
    Priority   int    `json:"priority"`
    RoutingKey string `json:"routing_key"`
    Exchange   string `json:"exchange"`
}

// GetTaskMessage retrieve and decode task messages from broker
func (cm *CeleryMessage) GetTaskMessage() *TaskMessage {
    // ensure content-type is 'application/json'
    if cm.ContentType != "application/json" {
        log.Println("unsupported content type " + cm.ContentType)
        return nil
    }
    // ensure body encoding is base64
    if cm.Properties.BodyEncoding != "base64" {
        log.Println("unsupported body encoding " + cm.Properties.BodyEncoding)
        return nil
    }
    // ensure content encoding is utf-8
    if cm.ContentEncoding != "utf-8" {
        log.Println("unsupported encoding " + cm.ContentEncoding)
        return nil
    }
    // decode body
    taskMessage, err := DecodeTaskMessage(cm.Body)
    if err != nil {
        log.Println("failed to decode task message")
        return nil
    }
    return taskMessage
}

// TaskMessage is celery-compatible message
// example:
/*
 {'origin': 'gen25458@iZuf6abi5ju3zkr4s85m6fZ',
  'lang': 'py',
  'task': 'App.aliyun_api.checkInstance',
  'group': None,
  'root_id': '93db85b1-28d5-4e19-9eee-80588fbfedd3',
  'delivery_info': {u'priority': 0, u'redelivered': None, u'routing_key': 'cpu', u'exchange': u''},
  'expires': None,
  'correlation_id': '93db85b1-28d5-4e19-9eee-80588fbfedd3',
  'retries': 0,
  'timelimit': [None, None],
  'argsrepr': "['005aff03e0524920b7a4d1386b886b04', 172800]",
  'eta': None,
  'parent_id': None,
  'reply_to': '227beeb6-f7b8-30a3-951c-d51bf55c0998',
  'id': '93db85b1-28d5-4e19-9eee-80588fbfedd3',
  'kwargsrepr': '{}'
  }
 */
type TaskMessage struct {
    ID      string                 `json:"id"`
    Task    string                 `json:"task"`
    Args    []interface{}          `json:"args"`   //argsrepr
    Kwargs  map[string]interface{} `json:"kwargs"` //kwargsrepr
    Retries int                    `json:"retries"`
    //MarshalJSON(deprecated in Go2, though) implements the json.Marshaler interface.
    //default use format RFC3339 = "2006-01-02T15:04:05Z07:00"
    ETA     time.Time `json:"eta"`
    Expires time.Time `json:"expires"`
}

func (tm *TaskMessage) reset() {
    tm.ID = generateUUID()
    tm.Task = ""
    tm.Args = nil
    tm.Kwargs = nil
}

var taskMessagePool = sync.Pool{
    New: func() interface{} {
        return &TaskMessage{
            ID:      generateUUID(),
            Retries: 0,
            Kwargs:  nil,
            //ETA:     time.Now().Format(time.RFC3339),
            ETA: time.Now(),
        }
    },
}

func getTaskMessage(task string) *TaskMessage {
    msg := taskMessagePool.Get().(*TaskMessage)
    msg.Task = task
    msg.Args = make([]interface{}, 0)
    msg.Kwargs = make(map[string]interface{})
    //msg.ETA = time.Now().Format(time.RFC3339)
    msg.ETA = time.Now()
    msg.Expires = time.Now().AddDate(1, 0, 0)
    return msg
}

func releaseTaskMessage(v *TaskMessage) {
    v.reset()
    taskMessagePool.Put(v)
}

// DecodeTaskMessage decodes base64 encrypted body and return TaskMessage object
func DecodeTaskMessage(encodedBody string) (*TaskMessage, error) {
    body, err := base64.StdEncoding.DecodeString(encodedBody)
    if err != nil {
        return nil, err
    }
    message := taskMessagePool.Get().(*TaskMessage)
    err = json.Unmarshal(body, message)
    if err != nil {
        return nil, err
    }
    return message, nil
}

// Encode returns base64 json encoded string
func (tm *TaskMessage) Encode() (string, error) {
    jsonData, err := json.Marshal(tm)
    logs.Debug(string(jsonData))
    if err != nil {
        return "", err
    }
    encodedData := base64.StdEncoding.EncodeToString(jsonData)
    return encodedData, err
}

// ResultMessage is return message received from broker
type ResultMessage struct {
    ID        string        `json:"task_id"`
    Status    string        `json:"status"`
    Traceback interface{}   `json:"traceback"`
    Result    interface{}   `json:"result"`
    Children  []interface{} `json:"children"`
}

func (rm *ResultMessage) reset() {
    rm.Result = nil
}

var resultMessagePool = sync.Pool{
    New: func() interface{} {
        return &ResultMessage{
            Status:    "SUCCESS",
            Traceback: nil,
            Children:  nil,
        }
    },
}

func getResultMessage(val interface{}) *ResultMessage {
    msg := resultMessagePool.Get().(*ResultMessage)
    msg.Result = val
    return msg
}

func getReflectionResultMessage(val *reflect.Value) *ResultMessage {
    msg := resultMessagePool.Get().(*ResultMessage)
    msg.Result = GetRealValue(val)
    return msg
}

func releaseResultMessage(v *ResultMessage) {
    v.reset()
    resultMessagePool.Put(v)
}

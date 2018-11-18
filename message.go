package gocelery

import (
    "encoding/base64"
    "log"
    "reflect"
    "sync"
    "time"
    "github.com/liamylian/jsontime"
)

// GLOBAL 替换掉 encoding/json
var (
    json    = jsontime.ConfigWithCustomTimeFormat
    ISO8601 = "2006-01-02T15:04:05"
)
// CeleryMessage is actual message to be sent to Redis
// 参考：http://docs.celeryproject.org/en/latest/internals/protocol.html#definition
// example:
/*
{
	"body": "W1tdLCB7InkiOiAyODc4LCAieCI6IDU0NTZ9LCB7ImNob3JkIjogbnVsbCwgImNhbGxiYWNrcyI6IG51bGwsICJlcnJiYWNrcyI6IG51bGwsICJjaGFpbiI6IG51bGx9XQ==",
	"headers": {
		"origin": "gen66194@DanceinydeMacBook-Pro.local",
		"root_id": "25abb5e6-d8c3-4b20-8dfb-7dc1be9ecf8f",
		"expires": null,
		"shadow": null,
		"id": "25abb5e6-d8c3-4b20-8dfb-7dc1be9ecf8f",
		"kwargsrepr": "{'y': 2878, 'x': 5456}",
		"lang": "py",
		"retries": 0,
		"task": "worker.add_reflect",
		"group": null,
		"timelimit": [null, null],
		"parent_id": null,
		"argsrepr": "()",
		"eta": null
	},
	"properties": {
		"priority": 0,
		"body_encoding": "base64",
		"correlation_id": "25abb5e6-d8c3-4b20-8dfb-7dc1be9ecf8f",
		"reply_to": "2f6f7ea8-dcc3-30a7-ae0c-4eb03ae4910c",
		"delivery_info": {
			"routing_key": "celery",
			"exchange": ""
		},
		"delivery_mode": 2,
		"delivery_tag": "a18604c0-5422-4592-877b-72e106744981"
	},
	"content-type": "application/json",
	"content-encoding": "utf-8"
}
 */
type CeleryMessage struct {
    // body是语言相关的
    //     object[] args,
    //    Mapping kwargs,
    //    Mapping embed {
    //        'callbacks': Signature[] callbacks,
    //        'errbacks': Signature[] errbacks,
    //        'chain': Signature[] chain,
    //        'chord': Signature chord_callback,
    //    }
    Body            string        `json:"body"`
    Headers         ST_Headers    `json:"headers"`
    Properties      ST_Properties `json:"properties"`
    ContentType     string        `json:"content-type"`
    ContentEncoding string        `json:"content-encoding"`
}

/*
	"properties": {
		"priority": 0,
		"body_encoding": "base64",
		"correlation_id": "25abb5e6-d8c3-4b20-8dfb-7dc1be9ecf8f",
		"reply_to": "2f6f7ea8-dcc3-30a7-ae0c-4eb03ae4910c",
		"delivery_info": {
			"routing_key": "celery",
			"exchange": ""
		},
		"delivery_mode": 2,
		"delivery_tag": "a18604c0-5422-4592-877b-72e106744981"
	},
 */
type ST_Properties struct {
    ContentEncoding string `json:"content_encoding"` // 事实上该字段移动到与properties并列的层级了
    ContentType     string `json:"content_type"`     // 事实上该字段移动到与properties并列的层级了
    CorrelationID   string `json:"correlation_id"`
    ReplyTo         string `json:"replay_to"`
    // 下面的在Celery文档中未曾提及
    BodyEncoding string             `json:"body_encoding"`
    Priority     int                `json:"priority"`
    DeliveryInfo CeleryDeliveryInfo `json:"delivery_info"`
    DeliveryMode int                `json:"delivery_mode"`
    DeliveryTag  string             `json:"delivery_tag"`
}

/*
真实json示例：
	"headers": {
		"origin": "gen66194@DanceinydeMacBook-Pro.local",
		"root_id": "25abb5e6-d8c3-4b20-8dfb-7dc1be9ecf8f",
		"expires": null,
		"shadow": null,
		"id": "25abb5e6-d8c3-4b20-8dfb-7dc1be9ecf8f",
		"kwargsrepr": "{'y': 2878, 'x': 5456}",
		"lang": "py",
		"retries": 0,
		"task": "worker.add_reflect",
		"group": null,
		"timelimit": [null, null],
		"parent_id": null,
		"argsrepr": "()",
		"eta": null
	},
 */
type ST_Headers struct {
    Lang     string    `json:"lang"`
    Task     string    `json:"task"`      // task name
    TaskId   string    `json:"id"`        // uuid
    RootId   string    `json:"root_id"`   // uuid
    ParentId string    `json:"parent_id"` // uuid
    Group    string    `json:"group_id"`  // uuid group_id
    Retries  int       `json:"retries"`
    ETA      time.Time `json:"eta" time_format:"2006-01-02T15:04:05"`
    Expires  time.Time `json:"expires" time_format:"2006-01-02T15:04:05"`
    Origin   string    `json:"origin"` // optional
    /*TODO
        'meth': string method_name,
        'shadow': string alias_name,
        'timelimit': (soft, hard),
        'argsrepr': str repr(args),
        'kwargsrepr': str repr(kwargs),
    */
}

func (cm *CeleryMessage) reset() {
    cm.Headers = ST_Headers{}
    cm.Body = ""
    cm.Properties.CorrelationID = generateUUID()
    cm.Properties.ReplyTo = generateUUID()
    cm.Properties.DeliveryTag = generateUUID()
}

//TODO: support customized delivery_info property
var celeryMessagePool = sync.Pool{
    New: func() interface{} {
        return &CeleryMessage{
            Body: "",
            Headers: ST_Headers{
                Lang: "golang",
            },
            ContentType:     "application/json",
            ContentEncoding: "utf-8",
            Properties: ST_Properties{
                BodyEncoding: "base64",
                // optional
                ReplyTo: generateUUID(),
                //DeliveryMode: 2,
                //DeliveryTag:  generateUUID(),
            },
        }
    },
}

func getDefaultCeleryDeliveryInfo() *CeleryDeliveryInfo {
    return &CeleryDeliveryInfo{
        RoutingKey: "celery",
        Exchange:   "",
    }
}
func NewCeleryDeliveryInfo(routingKey string, exchange string) *CeleryDeliveryInfo {
    return &CeleryDeliveryInfo{
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

func loadCeleryMessage(task *CeleryTask) *CeleryMessage {
    msg := celeryMessagePool.Get().(*CeleryMessage)
    msg.Body = task.EncodeBody();
    msg.Properties.DeliveryInfo = *getDefaultCeleryDeliveryInfo()
    msg.Properties.Priority = task.Priority
    msg.Properties.CorrelationID = task.Id
    msg.Headers.RootId = task.Id
    msg.Headers.TaskId = task.Id
    msg.Headers.Task = task.Task
    msg.Headers.ETA = task.ETA
    msg.Headers.Expires = task.Expires
    msg.Headers.Retries = task.Retries
    return msg;
}
func releaseCeleryMessage(v *CeleryMessage) {
    v.reset()
    celeryMessagePool.Put(v)
}

// CeleryDeliveryInfo represents deliveryinfo json
type CeleryDeliveryInfo struct {
    RoutingKey string `json:"routing_key"`
    Exchange   string `json:"exchange"`
}

// GetTaskMessage retrieve and decode task messages from broker
func (cm *CeleryMessage) GetTaskMessage() *CeleryTask {
    // ensure content-type is 'application/json'
    if cm.Properties.ContentType != "application/json" {
        log.Println("unsupported content type " + cm.Properties.ContentType)
        return nil
    }
    // ensure body encoding is base64
    if cm.Properties.ContentEncoding != "base64" {
        log.Println("unsupported body encoding " + cm.Properties.ContentEncoding)
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

// 按照Celery现有的python实现，不是将CeleryTask直接进行json序列化
type CeleryTask struct {
    Id      string                 `json:"id"`
    Task    string                 `json:"task"`
    Args    []interface{}          `json:"args"`   //argsrepr
    Kwargs  map[string]interface{} `json:"kwargs"` //kwargsrepr
    Retries int                    `json:"retries"`
    // Protocol 2: ISO8601，格式："2006-01-02T15:04:05"， 与RFC3339: "2006-01-02T15:04:05Z07:00"不同
    ETA      time.Time              `json:"eta" time_format:"2006-01-02T15:04:05"`
    Expires  time.Time              `json:"expires" time_format:"2006-01-02T15:04:05"`
    Priority int                    `json:"priority"`
    Embed    map[string]interface{} `json:"embed"`
}

func (tm *CeleryTask) reset() {
    tm.Id = generateUUID()
    tm.Task = ""
    tm.Args = nil
    tm.Kwargs = nil
}

var taskMessagePool = sync.Pool{
    New: func() interface{} {
        return &CeleryTask{
            Id:      generateUUID(),
            Retries: 0,
        }
    },
}

func getTaskObj(task string) *CeleryTask {
    msg := taskMessagePool.Get().(*CeleryTask)
    msg.Task = task
    msg.Args = make([]interface{}, 0)
    msg.Kwargs = make(map[string]interface{})
    msg.Embed = make(map[string]interface{})
    msg.Expires = time.Now().AddDate(1, 0, 0)
    return msg
}

func releaseTaskMessage(v *CeleryTask) {
    v.reset()
    taskMessagePool.Put(v)
}

// DecodeTaskMessage decodes base64 encrypted body and return Itf_CeleryTask object
func DecodeTaskMessage(encodedBody string) (*CeleryTask, error) {
    body, err := base64.StdEncoding.DecodeString(encodedBody)
    if err != nil {
        return nil, err
    }
    message := taskMessagePool.Get().(*CeleryTask)
    err = json.Unmarshal(body, message)
    if err != nil {
        return nil, err
    }
    return message, nil
}

// EncodeBody returns base64 json encoded string
func (tm *CeleryTask) EncodeBody() string {
    // python兼容版本
    // args, kwargs, embed = self._payload # _payload is body
    var payloadList = make([]interface{}, 3)
    payloadList[0] = tm.Args
    payloadList[1] = tm.Kwargs
    payloadList[2] = tm.Embed
    var jsonData, err = json.Marshal(payloadList)
    log.Printf("json body: %s", jsonData)
    if err != nil {
        log.Fatalf("celery message encode failed: %s", err.Error())
    }
    encodedData := base64.StdEncoding.EncodeToString(jsonData)
    return encodedData
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

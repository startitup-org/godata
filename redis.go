package godata

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type ChatAddress struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

type ChatMessage struct {
	Source      ChatAddress `json:"source"`
	Destination ChatAddress `json:"destination"`
	Message     string      `json:"msg"`
	Reply       string      `json:"reply,omitempty"`
	SocketId    string      `json:"sid,omitempty"`
	Hostname    string      `json:"host,omitempty"`
	Timestamp   time.Time   `json:"tm,omitempty"`
}

type PubSubMessage struct {
	Event string      `json:"event"`
	Data  ChatMessage `json:"data,omitempy"`
}

type RedisCommand struct {
	cmd  string
	key  string
	args []interface{}
}

type RedisCommands []RedisCommand

func (r *RedisCommands) Add(cmd string, key string, args ...interface{}) *RedisCommands {
	*r = append(*r, RedisCommand{cmd, key, args})
	return r
}

type RedisMessageHandler interface {
	HandleMessage(channel string, msg *PubSubMessage)
}

type Redis struct { //
	handler RedisMessageHandler
	conns   *redis.Pool
}

const (
	ChatChannel  = "startitup:chat"
	EventChannel = "startitup:event"
)

func NewRedis(h RedisMessageHandler, maxIdle int, address, password string) *Redis {
	db := Redis{h, redisNewPool(maxIdle, address, password)}
	fmt.Println("new Redis ready, conns", db.conns.ActiveCount())
	return &db
}

func (db *Redis) Conns() *redis.Pool {
	return db.conns
}

func (db *Redis) Publish(channel string, msg *PubSubMessage) {
	data, err := json.Marshal(msg)
	redisErrorHandler("Redis.Publish:marchal", err)
	_, err = db.Do("PUBLISH", channel, data)
	redisErrorHandler("Redis.Publish:Do", err)
	//fmt.Printf("Publish to %s: %s\n", channel, data)
}

func (db *Redis) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	c := db.conns.Get()
	defer c.Close()
	reply, err = c.Do(cmd, args...)
	redisErrorHandler("Redis.Do", err)
	return
}

func (db *Redis) MutliExec(cmds RedisCommands) (reply interface{}, err error) {
	c := db.conns.Get()
	defer c.Close()
	c.Send("MULTI")
	for _, cmd := range cmds {
		c.Send(cmd.cmd, append([]interface{}{cmd.key}, cmd.args...)...)
	}
	reply, err = c.Do("EXEC")
	redisErrorHandler("Redis.MutliExec:Do(Exec)", err)
	return
}

func (db *Redis) Run() {
	method := "Redis.Run"
	channels := []string{EventChannel, ChatChannel}
	fmt.Println(method, "begin:")

	c := db.conns.Get()
	defer c.Close()

	psc := redis.PubSubConn{c}
	for _, channel := range channels {
		psc.Subscribe(channel)
	}
	fmt.Println(method, "ready!, subscribe to channels", channels)

	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			//fmt.Printf("%s:message: %s: %s\n", method, v.Channel, v.Data)
			switch v.Channel {
			case EventChannel, ChatChannel:
				msg := &PubSubMessage{}
				err := json.Unmarshal(v.Data, msg)
				redisErrorHandler("Redis.Run:json.Unmarshal", err)
				db.handler.HandleMessage(v.Channel, msg)
			}
		case redis.Subscription:
			fmt.Printf("%s:subscription: %s: %s %d\n", method, v.Channel, v.Kind, v.Count)
		case error:
			fmt.Println(method, "got error:", v)
			return
		}
	}

	fmt.Println(method, "end!")
}

func redisNewPool(maxIdle int, address, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", address, redis.DialPassword(password))
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func redisErrorHandler(hd string, err error) {
	if err != nil {
		panic(fmt.Sprintln(hd, "error:", err))
	}
}

func redisBytes(reply interface{}, err error) []byte {
	val, err := redis.Bytes(reply, err)
	if err != redis.ErrNil {
		redisErrorHandler("redisBytes", err)
	}
	return val
}

func redisString(reply interface{}, err error) string {
	val, err := redis.String(reply, err)
	if err != redis.ErrNil {
		redisErrorHandler("redisString", err)
	}
	return val
}

func redisStrings(reply interface{}, err error) []string {
	val, err := redis.Strings(reply, err)
	if err != redis.ErrNil {
		redisErrorHandler("redisStrings", err)
	}
	return val
}

func redisInt(reply interface{}, err error) int {
	val, err := redis.Int(reply, err)
	if err != redis.ErrNil {
		redisErrorHandler("redisInt", err)
	}
	return val
}

func redisInt64(reply interface{}, err error) int64 {
	val, err := redis.Int64(reply, err)
	if err != redis.ErrNil {
		redisErrorHandler("redisInt64", err)
	}
	return val
}

func redisInts(reply interface{}, err error) []int {
	val, err := redis.Ints(reply, err)
	if err != redis.ErrNil {
		redisErrorHandler("redisInts", err)
	}
	return val
}

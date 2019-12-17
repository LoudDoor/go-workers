package workers

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type scheduled struct {
	keys   []string
	closed chan bool
	exit   chan bool
}

func (s *scheduled) start() {
	go (func() {
		for {
			select {
			case <-s.closed:
				return
			default:
			}

			s.poll()

			time.Sleep(time.Duration(Config.PollInterval) * time.Second)
		}
	})()
}

func (s *scheduled) quit() {
	close(s.closed)
}

func (s *scheduled) poll() {
	// conn := Config.Pool.Get()
	// defer conn.Close()
	conn := Config.Pool

	now := nowToSecondsWithNanoPrecision()

	for _, key := range s.keys {
		key = Config.Namespace + key
		for {
			messages, _ := conn.ZRangeByScore(key, &redis.ZRangeBy{
				Min:    "-inf",
				Max:    fmt.Sprintf("%.6f", now),
				Offset: 0,
				Count:  1,
			}).Result()
			// messages, _ := redis.Strings(conn.Do("zrangebyscore", key, "-inf", now, "limit", 0, 1))

			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[0])

			// if removed, _ := redis.Bool(conn.Do("zrem", key, messages[0])); removed {
			if removed, _ := conn.ZRem(key, messages[0]).Result(); removed > 0 {
				queue, _ := message.Get("queue").String()
				queue = strings.TrimPrefix(queue, Config.Namespace)
				message.Set("enqueued_at", nowToSecondsWithNanoPrecision())
				conn.LPush(Config.Namespace+"queue:"+queue, message.ToJson())
				// conn.Do("lpush", Config.Namespace+"queue:"+queue, message.ToJson())
			}
		}
	}
}

func newScheduled(keys ...string) *scheduled {
	return &scheduled{keys, make(chan bool), make(chan bool)}
}

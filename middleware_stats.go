package workers

import (
	"time"
)

type MiddlewareStats struct{}

func (l *MiddlewareStats) Call(queue string, message *Msg, next func() bool) (acknowledge bool) {
	defer func() {
		if e := recover(); e != nil {
			incrementStats("failed")
			panic(e)
		}
	}()

	acknowledge = next()

	incrementStats("processed")

	return
}

func incrementStats(metric string) {
	// conn := Config.Pool.Get()
	// defer conn.Close()
	conn := Config.Pool
	pipe := conn.Pipeline()

	today := time.Now().UTC().Format("2006-01-02")

	pipe.Incr(Config.Namespace + "stat:" + metric)
	pipe.Incr(Config.Namespace + "stat:" + metric + ":" + today)

	// if _, err := conn.Do("exec"); err != nil {
	if _, err := pipe.Exec(); err != nil {
		Logger.Println("couldn't save stats:", err)
	}
}

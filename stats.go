package workers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-redis/redis"
)

type stats struct {
	Processed int         `json:"processed"`
	Failed    int         `json:"failed"`
	Jobs      interface{} `json:"jobs"`
	Enqueued  interface{} `json:"enqueued"`
	Retries   int64       `json:"retries"`
}

func Stats(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	jobs := make(map[string][]*map[string]interface{})
	enqueued := make(map[string]string)

	for _, m := range managers {
		queue := m.queueName()
		jobs[queue] = make([]*map[string]interface{}, 0)
		enqueued[queue] = ""
		for _, worker := range m.workers {
			message := worker.currentMsg
			startedAt := worker.startedAt

			if message != nil && startedAt > 0 {
				jobs[queue] = append(jobs[queue], &map[string]interface{}{
					"message":    message,
					"started_at": startedAt,
				})
			}
		}
	}

	stats := stats{
		0,
		0,
		jobs,
		enqueued,
		0,
	}

	// conn := Config.Pool.Get()
	// defer conn.Close()
	conn := Config.Pool
	pipe := conn.Pipeline()

	processed := pipe.Get(Config.Namespace + "stat:processed")
	failed := pipe.Get(Config.Namespace + "stat:failed")
	zcard := pipe.ZCard(Config.Namespace + RETRY_KEY)

	results := make([]*redis.IntCmd, 0)
	for key, _ := range enqueued {
		results = append(results, conn.LLen(fmt.Sprintf("%squeue:%s", Config.Namespace, key)))
	}

	_, err := pipe.Exec()

	if err != nil {
		Logger.Println("couldn't retrieve stats:", err)
	}

	procCount, procError := processed.Result()
	if procError != nil {
		stats.Processed, _ = strconv.Atoi(procCount)
	}

	failCount, failError := failed.Result()
	if failError != nil {
		stats.Failed, _ = strconv.Atoi(failCount)
	}

	zCount, zError := zcard.Result()
	if zError != nil {
		stats.Retries = zCount
	}

	if len(results) == len(enqueued) {
		for index, cmd := range results {
			key := strconv.Itoa(index)
			enqueued[key] = fmt.Sprintf("%d", cmd.Val())
		}
	}

	body, _ := json.MarshalIndent(stats, "", "  ")
	fmt.Fprintln(w, string(body))
}

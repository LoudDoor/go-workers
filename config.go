package workers

import (
	"crypto/tls"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

type config struct {
	processId    string
	Namespace    string
	PollInterval int
	Pool         *redis.Client
	Fetch        func(queue string) Fetcher
}

var Config *config

func Configure(options map[string]string) {
	var namespace string
	var pollInterval int

	if options["server"] == "" {
		panic("Configure requires a 'server' option, which identifies a Redis instance")
	}

	if options["process"] == "" {
		panic("Configure requires a 'process' option, which uniquely identifies this instance")
	}

	if options["pool"] == "" {
		options["pool"] = "1"
	}

	if options["namespace"] != "" {
		namespace = options["namespace"] + ":"
	}

	if seconds, err := strconv.Atoi(options["poll_interval"]); err == nil {
		pollInterval = seconds
	} else {
		pollInterval = 15
	}

	Config = &config{
		processId:    options["process"],
		Namespace:    namespace,
		PollInterval: pollInterval,
		Pool:         createRedisPool(options),
		Fetch: func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
}

func createRedisPool(options map[string]string) *redis.Client {
	poolSize, _ := strconv.Atoi(options["pool"])
	dbID, _ := strconv.Atoi(options["database"])

	redisClientOpts := &redis.Options{
		Addr:         options["server"],
		DB:           dbID,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxRetries:   3,

		PoolSize:           poolSize,
		MinIdleConns:       3,
		MaxConnAge:         60 * time.Second,
		IdleTimeout:        240 * time.Second,
		IdleCheckFrequency: 500 * time.Millisecond,
	}

	if "" != options["password"] {
		redisClientOpts.Password = options["password"]
	}

	if useTLS, ok := options["useTLS"]; ok {
		if "yes" == useTLS {
			redisClientOpts.TLSConfig = &tls.Config{}
		}
	}

	return redis.NewClient(redisClientOpts)
}

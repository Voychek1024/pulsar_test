package main

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/montanaflynn/stats"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) <= 1 {
		fmt.Println("Invalid Args\nUsage: ./consumer configName")
		os.Exit(2)
	}

	var err error
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println(fmt.Sprintf("Cannot get HOSTNAME err: %v", err))
		hostname = ""
	}

	logger, _ := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: true,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:      "json",
		EncoderConfig: zap.NewProductionEncoderConfig(),
		OutputPaths:   []string{fmt.Sprintf("./logs/consumer.dev.log.%s_%s", time.Now().Format("20060102"), hostname)},
	}.Build()

	defer func(logger *zap.Logger) {
		err = logger.Sync()
		if err != nil {
			println(fmt.Sprintf("Cannot flush logger, %v", err))
		}
	}(logger)

	log := logger.Sugar()

	viper.SetConfigName(os.Args[1])
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetConfigType("yml")
	if err = viper.ReadInConfig(); err != nil {
		log.Fatalln(fmt.Sprintf("Failed reading config file %v", err))
	}

	// Pulsar Settings
	tenant := viper.GetString("PULSAR_SETTINGS.tenant")
	namespace := viper.GetString("PULSAR_SETTINGS.namespace")
	topic := viper.GetString("PULSAR_SETTINGS.topic")
	brokerAddress := viper.GetString("PULSAR_SETTINGS.broker_address")
	tokenCtrl := viper.GetString("PULSAR_SETTINGS.token")
	// Consumer Settings
	subscriptionName := viper.GetString("CONSUMER_SETTINGS.subscription_name")
	logFlag := viper.GetBool("CONSUMER_SETTINGS.log_detail")
	logCounterFlag := viper.GetBool("CONSUMER_SETTINGS.log_counter")
	threadNum := viper.GetInt("CONSUMER_SETTINGS.thread")
	// Producer Settings
	payload := viper.GetString("PRODUCER_SETTINGS.payload")

	options := pulsar.ClientOptions{
		URL:               brokerAddress,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 5 * time.Second,
	}
	if len(tokenCtrl) > 0 {
		options.Authentication = pulsar.NewAuthenticationToken(func(fileName string) string {
			var token []byte
			token, err = os.ReadFile(fmt.Sprintf("./tokens/%s", fileName))
			if err != nil {
				log.Fatalln(fmt.Sprintf("Cannot open token file: %s - %s", fileName, err))
			}
			return string(token)
		}(tokenCtrl))
	}

	var realTopics []string
	var namespaceSlice []string
	if strings.Contains(namespace, ",") {
		namespaceSlice = strings.Split(namespace, ",")
		for _, nm := range namespaceSlice {
			realTopics = append(realTopics, fmt.Sprintf("persistent://%s/%s_%s/%s", tenant, tenant, nm, topic))
		}
	} else {
		namespaceSlice = append(namespaceSlice, namespace)
		realTopics = []string{fmt.Sprintf("persistent://%s/%s_%s/%s", tenant, tenant, namespace, topic)}
	}

	client, err := pulsar.NewClient(options)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Could not instantiate Pulsar client: %v", err))
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topics:                      realTopics,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		NackRedeliveryDelay:         500 * time.Millisecond,
	})
	if err != nil {
		log.Fatalln(fmt.Sprintf("Could not instantiate Pulsar comsumer: %v", err))
	}

	go func() {
		prometheusPort := 2112
		log.Infoln(fmt.Sprintf("Starting Prometheus metrics at http://localhost:%v/metrics", prometheusPort))
		http.Handle("/metrics", promhttp.Handler())
		err = http.ListenAndServe(":"+strconv.Itoa(prometheusPort), nil)
		if err != nil {
			log.Errorln(fmt.Sprintf("Could not serve promhttp: %v", err))
		}
	}()

	sig := make(chan os.Signal, 1)
	done := make(chan bool)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		{
			fmt.Println("Triggered clean up process...")
			close(done)
		}
	}()

	// local stats (TPS, P50, P99)
	var msgReceived = map[string]int64{}
	var msgLatency = map[string][]float64{}
	for _, nm := range namespaceSlice {
		msgReceived[nm] = int64(0)
	}
	mux := sync.Mutex{}
	tick := time.NewTicker(1 * time.Second) // log TPS, P50, P99 per sec
	defer tick.Stop()
	// parsing namespace from topic: persistent://tenant/tenant_(namespace)/topic
	r := regexp.MustCompile(`/\w+_(\w+)/`)

	var wg sync.WaitGroup
	wg.Add(threadNum)
	for i := 0; i < threadNum; i++ {
		go func(i int) {
			defer wg.Done()
			threadFuncReceive(i, consumer, done, &mux, msgReceived, msgLatency, log, logCounterFlag, logFlag, r, hostname, payload)
		}(i)
	}

	for {
		select {
		case <-done:
			wg.Wait()
			fmt.Println("All consumer threads shutdown gracefully")
			consumer.Close()
			fmt.Println("Consumer shutdown gracefully")
			logCounter(msgReceived, msgLatency, &mux, log, logCounterFlag)
			time.Sleep(120 * time.Second) // wait for prometheus scrape
			return
		case <-tick.C:
			logCounter(msgReceived, msgLatency, &mux, log, logCounterFlag)
		}
	}
}

func logCounter(counter map[string]int64, latency map[string][]float64, mux *sync.Mutex, logger *zap.SugaredLogger, logCounterFlag bool) {
	var p50 float64
	var p99 float64
	var err error
	if logCounterFlag == true {
		mux.Lock()
		for k, v := range counter {
			if v > 0 {
				p50, err = stats.Percentile(latency[k], 50)
				if err != nil {
					logger.Errorln(fmt.Sprintf("Cannot calculate p50 percentile: %v", err))
					goto OUT
				}
				p99, err = stats.Percentile(latency[k], 99)
				if err != nil {
					logger.Errorln(fmt.Sprintf("Cannot calculate p99 percentile: %v", err))
					goto OUT
				}
				logger.Infoln(fmt.Sprintf("Perform stat: (%s - %v ~ %.02f - %.02f)", k, v, p50, p99))
			OUT:
				counter[k] = 0
				latency[k] = nil
			}
		}
		mux.Unlock()
	}
}

func threadFuncReceive(i int, consumer pulsar.Consumer, done chan bool, mux *sync.Mutex, received map[string]int64, latency map[string][]float64, log *zap.SugaredLogger, logCounterFlag bool, logFlag bool, r *regexp.Regexp, hostname string, payload string) {
	fmt.Println(fmt.Sprintf("Consumer thread %d started...", i))
	for {
		select {
		case <-done:
			fmt.Println(fmt.Sprintf("Consumer thread %d shutdown gracefully", i))
			return
		case cm, ok := <-consumer.Chan():
			{
				if !ok {
					if cm.Message != nil {
						consumer.Nack(cm.Message)
						log.Errorln(fmt.Sprintf("thread %d: Nacked message: %v", i, cm.Message))
					}
					log.Errorln(fmt.Sprintf("thread %d: Failed to consume message", i))
				} else {
					// record subTime and parse pubTime from payload
					subTimestamp := time.Now().Format("2006-01-02T15:04:05.000000")
					pubTimestamp := strings.Split(string(cm.Message.Payload()), "|")[0]
					subTime, _ := time.Parse("2006-01-02T15:04:05.000000", subTimestamp)
					pubTime, err := time.Parse("2006-01-02T15:04:05.000000", pubTimestamp)
					if err != nil {
						log.Warnln(fmt.Sprintf("Invalid pubTimestamp: %s, %v", pubTimestamp, err))
					}

					// parse tag from topic and do statistics
					currentTag := r.FindStringSubmatch(cm.Message.Topic())[1]
					if logCounterFlag == true {
						mux.Lock()
						received[currentTag]++
						latency[currentTag] = append(latency[currentTag], float64(subTime.Sub(pubTime).Microseconds())/1000)
						mux.Unlock()
					}

					// payloadSend:                              (send_timestamp)|(send_thread_id)|(send_namespace)|(payload:real_payload)|(send_hostname)
					// payloadRecv (message_id)|(recv_timestamp)|(send_timestamp)|(send_thread_id)|(send_namespace)|(payload:real_payload)|(send_hostname)|(recv_hostname)
					payloadRecv := fmt.Sprintf("DEBUG:root:(%s,go,%d)|%s|%s|%s",
						strings.ReplaceAll(cm.Message.ID().String(), ",", ""),
						i,
						subTimestamp,
						string(cm.Message.Payload()),
						hostname,
					)

					if logFlag == true {
						payloadLog := strings.ReplaceAll(payloadRecv, payload, "")
						log.Infoln(payloadLog)
					}

					err = consumer.Ack(cm.Message)
					if err != nil {
						log.Errorln("Failed to send ACK: %v", err)
					}
				}
			}
		}
	}
}

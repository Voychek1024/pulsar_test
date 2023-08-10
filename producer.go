package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type funcMapping map[string]interface{}

var funcMappingLocal = funcMapping{}

func main() {
	if len(os.Args) <= 1 {
		fmt.Println("Invalid Args\nUsage: ./producer configName")
		os.Exit(2)
	}

	var realRate int
	var err error
	if len(os.Args) == 3 {
		realRate, err = strconv.Atoi(os.Args[2])
		if (err != nil) || (realRate < 0) {
			fmt.Println("Invalid Args\nUsage: ./producer configName publishRatio(int, >0, msg/s)")
			os.Exit(2)
		}
	} else {
		// set unlimited
		realRate = 0
	}

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
		OutputPaths:   []string{fmt.Sprintf("./logs/producer.dev.log.%s_%s", time.Now().Format("20060102"), hostname)},
	}.Build()
	log := logger.Sugar()

	defer func(logger *zap.Logger) {
		err = logger.Sync()
		if err != nil {
			println(fmt.Sprintf("Cannot flush logger, %v", err))
		}
	}(logger)

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
	// Producer Settings
	threadNum := viper.GetInt("PRODUCER_SETTINGS.thread")
	payload := viper.GetString("PRODUCER_SETTINGS.payload")
	productionName := viper.GetString("PRODUCER_SETTINGS.production_name")
	logFlag := viper.GetBool("PRODUCER_SETTINGS.log_detail")
	funcName := viper.GetString("PRODUCER_SETTINGS.func_name")

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

	client, err := pulsar.NewClient(options)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Could not instantiate Pulsar client: %v", err))
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

	var producers = map[string]pulsar.Producer{}
	var producer pulsar.Producer
	for i, nm := range namespaceSlice {
		producerOptions := pulsar.ProducerOptions{
			Topic:       realTopics[i],
			SendTimeout: 400 * time.Millisecond,
			//CompressionType: pulsar.LZ4,
		}
		if len(productionName) > 0 {
			producerOptions.Name = fmt.Sprintf("%s", productionName)
		}

		producer, err = client.CreateProducer(producerOptions)
		if err != nil {
			log.Fatalln(fmt.Sprintf("Could not instantiate Pulsar producer: %v", err))
		}
		producers[nm] = producer
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
			fmt.Println("Triggered clean up process ...")
			close(done)
		}
	}()

	if (realRate > 0) && (threadNum > 0) {
		realRate = realRate / threadNum
		fmt.Println(fmt.Sprintf("Dispatch ratio to each thread at: %d (msg/s)", realRate))
	}

	funcMappingLocal = map[string]interface{}{
		"SendAsync": threadFuncSendAsync,
		"Send":      threadFuncSend,
	}

	var wg sync.WaitGroup
	wg.Add(threadNum)
	for i := 0; i < threadNum; i++ {
		go func(i int) {
			defer wg.Done()
			_, err = Call(funcName, i, done, realRate, namespaceSlice, producers, payload, hostname, log, logFlag)
			if err != nil {
				fmt.Println(fmt.Sprintf("Call %s error: %v", funcName, err))
				return
			}
		}(i)
	}

	for {
		select {
		case <-done:
			{
				wg.Wait()
				fmt.Println("All producer threads shutdown gracefully")
				for i, p := range producers {
					p.Close()
					fmt.Println(fmt.Sprintf("Producer %s shutdown gracefully", i))
				}
				fmt.Println("All producers shutdown gracefully")
				time.Sleep(120 * time.Second) // wait for prometheus scrape
				return
			}
		}
	}
}

func Call(funcName string, params ...interface{}) ([]reflect.Value, error) {
	var err error = nil

	f := reflect.ValueOf(funcMappingLocal[funcName])
	if len(params) != f.Type().NumIn() {
		err = errors.New(fmt.Sprintf("Not enough params to call %s: (require:%d - actual:%d)", funcName, f.Type().NumIn(), len(params)))
		return nil, err
	}

	inParams := make([]reflect.Value, len(params))
	for i, param := range params {
		if reflect.TypeOf(param) != f.Type().In(i) {
			err = errors.New(fmt.Sprintf("Not valid params[%d] to call %s: (require:%v - actual:%v)", i, funcName, f.Type().In(i), reflect.TypeOf(param)))
			return nil, err
		}
		inParams[i] = reflect.ValueOf(param)
	}

	result := f.Call(inParams)

	return result, err
}

func threadFuncSendAsync(i int, done chan bool, realRate int, namespaces []string, producers map[string]pulsar.Producer, payload string, hostname string, log *zap.SugaredLogger, logFlag bool) {
	fmt.Println(fmt.Sprintf("Producer thread %d started SendAsync...", i))

	limit := rate.Every(time.Duration(float64(time.Second) / float64(realRate)))
	rateLimiter := rate.NewLimiter(limit, realRate)
	fmt.Println(fmt.Sprintf("Producer thread %d: Initiated publish rate at: %d", i, realRate))

	for {
		select {
		case <-done:
			fmt.Println(fmt.Sprintf("Producer thread %d shutdown gracefully", i))
			return
		default:
			{
				// publishRatio limit
				if realRate > 0 {
					_ = rateLimiter.Wait(context.TODO())
				}

				// random pick a tag and do sendAsync
				pickTag := namespaces[rand.Intn(len(producers))]

				// payloadSend: (send_timestamp)|(send_thread_id)|(send_namespace)|(payload:real_payload)|(send_hostname)
				payloadSend := fmt.Sprintf("%s|%d|%s|payload:%s|%s",
					time.Now().Format("2006-01-02T15:04:05.000000"), i, pickTag, payload, hostname)
				producers[pickTag].SendAsync(
					context.Background(),
					&pulsar.ProducerMessage{
						Payload: []byte(payloadSend),
					},
					func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
						if err != nil {
							log.Warnln(fmt.Sprintf("Failed to publish message: %v", err))
						} else {
							if logFlag == true {
								payloadLog := strings.ReplaceAll(payloadSend, payload, "")
								log.Infoln(payloadLog)
							}
						}
					})
			}
		}
	}
}

func threadFuncSend(i int, done chan bool, realRate int, namespaces []string, producers map[string]pulsar.Producer, payload string, hostname string, log *zap.SugaredLogger, logFlag bool) {
	fmt.Println(fmt.Sprintf("Producer thread %d started Send...", i))

	limit := rate.Every(time.Duration(float64(time.Second) / float64(realRate)))
	rateLimiter := rate.NewLimiter(limit, realRate)
	fmt.Println(fmt.Sprintf("Producer thread %d: Initiated publish rate at: %d", i, realRate))

	for {
		select {
		case <-done:
			fmt.Println(fmt.Sprintf("Producer thread %d shutdown gracefully", i))
			return
		default:
			{
				// publishRatio limit
				if realRate > 0 {
					_ = rateLimiter.Wait(context.TODO())
				}

				// random pick a tag and do sendAsync
				pickTag := namespaces[rand.Intn(len(producers))]

				// payloadSend: (send_timestamp)|(send_thread_id)|(send_namespace)|(payload:real_payload)|(send_hostname)
				payloadSend := fmt.Sprintf("%s|%d|%s|payload:%s|%s",
					time.Now().Format("2006-01-02T15:04:05.000000"), i, pickTag, payload, hostname)
				_, err := producers[pickTag].Send(
					context.Background(),
					&pulsar.ProducerMessage{
						Payload: []byte(payloadSend),
					},
				)
				if err != nil {
					log.Warnln(fmt.Sprintf("Failed to publish message: %v", err))
				}
				if logFlag == true {
					payloadLog := strings.ReplaceAll(payloadSend, payload, "")
					log.Infoln(payloadLog)
				}
			}
		}
	}
}

package main

import (
	"os"
	"os/signal"
	"runtime"

	"awesomeProject/beacon/mqtt_network/broker_core_module"

	"go.uber.org/zap"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	//logger, err := zap.NewDevelopment(zap.AddStacktrace(zap.DebugLevel))
	logger, err := zap.NewProduction(zap.AddStacktrace(zap.PanicLevel))
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	b, err := broker_core_module.NewBroker(broker_core_module.WithBrokerLogger(logger))
	if err != nil {
		panic(err)
	}

	if err = b.StartListening(); err != nil {
		panic(err)
	}

	waitForSignal()
}

func waitForSignal() {
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Kill, os.Interrupt)
	<-signalChan
	signal.Stop(signalChan)
	//return s
}

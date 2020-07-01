package main

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"awesomeProject/beacon/mqtt_network/broker_p2p_module"

	"github.com/spf13/pflag"
)

var (
	hostFlag     = pflag.IPP("host", "h", nil, "p2p network binding host")
	portFlag     = pflag.Uint16P("port", "p", 15666, "p2p network binding port")
	mqttPortFlag = pflag.Uint16P("mqtt_port", "m", 1883, "mqtt broker binding port")
	debugFlag    = pflag.BoolP("debug", "d", false, "logger enable debug mode")
)

func main() {
	// Parse flags/options.
	pflag.Parse()

	fmt.Printf("The configuration infomation => host: %s, port: %d, mqtt port: %d \n", hostFlag.String(), *portFlag, *mqttPortFlag)
	if *hostFlag == nil {
		localIP, err := getLocalFirstIPAddress()
		if err != nil {
			panic(err)
		}
		*hostFlag = localIP
		fmt.Printf("Get first local IP [%s] for host due to the host in the command's configuration is nil. \n", localIP.String())
	}

	if *debugFlag == true {
		fmt.Printf("The Logger debug mode enable. \n")
	}

	if len(pflag.Args()) > 0 {
		fmt.Printf("The p2p network bootstrap address is [%s] \n", strings.Join(pflag.Args(), ", "))
	}

	strLine := "—————————————————————————————————————————————————————————————————————————————————————————————————————————— \n"
	fmt.Printf(strLine)

	if *hostFlag == nil || *portFlag == 0 || *mqttPortFlag == 0 {
		pflag.PrintDefaults()
		return
	}

	// Create a new configured node.
	// Command line : ./mqtt_service_p2p -h 127.0.0.1 -p 9000 -m 1883
	broker_p2p_module.ServiceWithFlag(*hostFlag, *portFlag, "", *hostFlag, *mqttPortFlag, "", *debugFlag, pflag.Args()...)
}

func getLocalFirstIPAddress() (net.IP, error) {
	addrList, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range addrList {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil && ipNet.IP.IsGlobalUnicast() {
				return ipNet.IP, nil
			}
		}
	}
	return nil, errors.New("valid local IP not found")
}

package main

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/spf13/pflag"
)

var (
	hostFlag    = pflag.IPP("host", "h", nil, "p2p network binding host")
	portFlag    = pflag.Uint16P("port", "p", 15666, "p2p network binding port")
	addressFlag = pflag.StringP("address", "a", "", "publicly reachable network address")
)

func main() {
	// Parse flags/options.
	pflag.Parse()

	fmt.Printf("The configuration infomation => host: %s, port: %d \n", hostFlag.String(), *portFlag)
	if *hostFlag == nil {
		localIP, err := getLocalFirstIPAddress()
		if err != nil {
			panic(err)
		}
		*hostFlag = localIP
		fmt.Printf("Get first local IP [%s] for host due to the host in the command's configuration is nil. \n", localIP.String())
	}

	if len(pflag.Args()) > 0 {
		fmt.Printf("The p2p network bootstrap address is [%s] \n", strings.Join(pflag.Args(), ", "))
	}

	strLine := "——————————————————————————————————————————————————————————————————————————————————————————————————— \n"
	fmt.Printf(strLine)

	if *hostFlag == nil || *portFlag == 0 {
		pflag.PrintDefaults()
		return
	}

	serviceWithFlag(*hostFlag, *portFlag, *addressFlag, pflag.Args()...)

	// Empty println.
	println()
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

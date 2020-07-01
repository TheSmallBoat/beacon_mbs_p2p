
build for linux in mac osx
~~~~~~~~~~~~~~~~~~~~~~~~~~~
go clean
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "../docker/mbs_p2p_linux" -ldflags "-w"

build docker image
~~~~~~~~~~~~~~~~~~~~~~~~
docker build -m 4g -t beacon/mbs_p2p:03.20 -t beacon/mbs_p2p:03.20.alpine -t beacon/mbs_p2p:03.20.alpine.3.8 ./

or
~~~~~~~
docker build -m 4g -t beacon/mbs_p2p:03.20.alpine.3.8 ./

docker usage
~~~~~~~~~~~~~~~~~~~~~~~~
docker run -d -it --name beacon_mbs_p2p_n1 -p 1883:1883 beacon/mbs_p2p:03.20.alpine.3.8
docker run -d -it --name beacon_mbs_p2p_n2 -p 2883:1883 beacon/mbs_p2p:03.20.alpine.3.8
docker run -d -it --name beacon_mbs_p2p_n3 -p 3883:1883 beacon/mbs_p2p:03.20.alpine.3.8
docker run -d -it --name beacon_mbs_p2p_n4 -p 4883:1883 beacon/mbs_p2p:03.20.alpine.3.8
docker run -d -it --name beacon_mbs_p2p_n5 -p 5883:1883 beacon/mbs_p2p:03.20.alpine.3.8
docker run -d -it --name beacon_mbs_p2p_n6 -p 6883:1883 beacon/mbs_p2p:03.20.alpine.3.8
docker run -d -it --name beacon_mbs_p2p_n7 -p 7883:1883 beacon/mbs_p2p:03.20.alpine.3.8
docker run -d -it --name beacon_mbs_p2p_n8 -p 8883:1883 beacon/mbs_p2p:03.20.alpine.3.8
docker run -d -it --name beacon_mbs_p2p_n9 -p 9883:1883 beacon/mbs_p2p:03.20.alpine.3.8
docker run -d -it --name beacon_mbs_p2p_n10 -p 10883:1883 beacon/mbs_p2p:03.20.alpine.3.8
... ...

User/Group
The image runs mbs_p2p under the beacon user and group, which are created with a uid and gid of 1883.

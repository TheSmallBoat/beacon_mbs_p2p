package broker_core_module

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

func (b *Broker) OnlineOfflineNotification(clientID string, online bool) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/Broker/Connection/Clients/" + clientID
	packet.Qos = QosAtMostOnce
	packet.Payload = []byte(fmt.Sprintf(`{"clientID":"%s","online":%v,"timestamp":"%s"}`, clientID, online, time.Now().UTC().Format(time.RFC3339)))

	b.SubmitPublishPacketsWorkTask(packet)
}

func (b *Broker) TopicActionsMetricsNotification(brokerIdStr string, infoList []string) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/metrics/topic_actions/broker/" + brokerIdStr
	packet.Qos = QosAtMostOnce
	packet.Payload = []byte(fmt.Sprintf(`{"broker_id":"%s","timestamp":"%s","detail":%s}`, brokerIdStr, time.Now().UTC().Format(time.RFC3339), getInfoStringFromList(infoList)))
	b.SubmitPublishPacketsWorkTask(packet)
}

func (b *Broker) ForwardPacketsMetricsNotification(brokerIdStr string, targetBrokerIdStr string, infoList []string) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/metrics/forward_packets/broker/" + brokerIdStr + "_" + targetBrokerIdStr
	packet.Qos = QosAtMostOnce
	packet.Payload = []byte(fmt.Sprintf(`{"broker_id":"%s","target_broker_id":"%s","timestamp":"%s","detail":%s}`, brokerIdStr, targetBrokerIdStr, time.Now().UTC().Format(time.RFC3339), getInfoStringFromList(infoList)))
	b.SubmitPublishPacketsWorkTask(packet)
}

func GetInfoStringFromList(infoList []string) string {
	return getInfoStringFromList(infoList)
}

func getInfoStringFromList(infoList []string) string {
	infoStr := "{"
	for idx, inf := range infoList {
		if idx > 0 {
			infoStr += ","
		}
		infoStr += fmt.Sprintf(`"%s":%s`, humanize.Ordinal(idx+1), inf)
	}
	infoStr += "}"
	return infoStr
}

func (b *Broker) MessageOverP2PMetricsNotification(brokerIdStr string, lastMinuteMetricsInfo string, grandTotalMetricsInfo string) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/metrics/message_over_p2p/broker/" + brokerIdStr
	packet.Qos = QosAtMostOnce
	packet.Payload = []byte(fmt.Sprintf(`{"broker_id":"%s","timestamp":"%s","last_minute":%s,"grand_total":%s}`, brokerIdStr, time.Now().UTC().Format(time.RFC3339), lastMinuteMetricsInfo, grandTotalMetricsInfo))
	b.SubmitPublishPacketsWorkTask(packet)
}

func (b *Broker) WorkPoolMetricsNotification(brokerIdStr string, metricsInfo string) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/metrics/fixed_worker_pool/broker/" + brokerIdStr
	packet.Qos = QosAtMostOnce
	packet.Payload = []byte(fmt.Sprintf(`{"broker_id":"%s","timestamp":"%s","metrics":%s}`, brokerIdStr, time.Now().UTC().Format(time.RFC3339), metricsInfo))
	b.SubmitPublishPacketsWorkTask(packet)
}

func (b *Broker) PacketForwardMetricsNotification(brokerIdStr string, metricsInfo string) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/metrics/packet_forward/broker/" + brokerIdStr
	packet.Qos = QosAtMostOnce
	packet.Payload = []byte(fmt.Sprintf(`{"broker_id":"%s","timestamp":"%s","metrics":%s}`, brokerIdStr, time.Now().UTC().Format(time.RFC3339), metricsInfo))
	b.SubmitPublishPacketsWorkTask(packet)
}

func (b *Broker) PeerNodeNotification(brokerIdStr string, mod string, info string) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/info/" + mod + "/broker/" + brokerIdStr
	packet.Qos = QosAtMostOnce
	packet.Payload = []byte(fmt.Sprintf(`{"broker_id":"%s","timestamp":"%s","%s_info":%s}`, brokerIdStr, time.Now().UTC().Format(time.RFC3339), mod, info))
	b.SubmitPublishPacketsWorkTask(packet)
}

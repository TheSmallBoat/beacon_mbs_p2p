package sessions

import (
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/stretchr/testify/require"
)

func TestSessionInit(t *testing.T) {
	sess := &Session{}
	connMessage := newConnectMessage()

	err := sess.Initialize(connMessage)
	require.NoError(t, err)
	require.Equal(t, true, sess.initialized)
	require.Equal(t, connMessage.WillQos, sess.connectMessage.WillQos)
	require.Equal(t, connMessage.ProtocolVersion, sess.connectMessage.ProtocolVersion)
	require.Equal(t, connMessage.CleanSession, sess.connectMessage.CleanSession)
	require.Equal(t, connMessage.ClientIdentifier, sess.connectMessage.ClientIdentifier)
	require.Equal(t, connMessage.Keepalive, sess.connectMessage.Keepalive)
	require.Equal(t, connMessage.WillTopic, sess.connectMessage.WillTopic)
	require.Equal(t, connMessage.WillMessage, sess.connectMessage.WillMessage)
	require.Equal(t, connMessage.Username, sess.connectMessage.Username)
	require.Equal(t, connMessage.Password, sess.connectMessage.Password)
	require.Equal(t, "will", sess.connectMessage.WillTopic)
	require.Equal(t, connMessage.WillQos, sess.connectMessage.WillQos)

	err = sess.AddTopic("test", 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(sess.topics))

	topicList, qosList, errT := sess.Topics()
	require.NoError(t, errT)
	require.Equal(t, 1, len(topicList))
	require.Equal(t, 1, len(qosList))
	require.Equal(t, "test", topicList[0])
	require.Equal(t, 1, int(qosList[0]))

	err = sess.RemoveTopic("test")
	require.NoError(t, err)
	require.Equal(t, 0, len(sess.topics))
}

func TestSessionRetainMessage(t *testing.T) {
	sess := &Session{}
	connMessage := newConnectMessage()
	err := sess.Initialize(connMessage)
	require.NoError(t, err)

	msg := newPublishMessage(1234, 1)
	err = sess.RetainMessage(msg)
	require.NoError(t, err)
	require.Equal(t, sess.Retained, msg)
}

func newConnectMessage() *packets.ConnectPacket {
	msg := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	msg.WillQos = 1
	msg.ProtocolVersion = 4
	msg.CleanSession = true
	msg.ClientIdentifier = "Mqtt-Network-Node-ID"
	msg.Keepalive = 10
	msg.WillTopic = "will"
	msg.WillMessage = []byte("send me home")
	msg.Username = "MQTT-NET-USER"
	msg.Password = []byte("VerySecret")

	return msg
}

func newPublishMessage(pktId uint16, qos byte) *packets.PublishPacket {
	msg := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	msg.MessageID = pktId
	msg.Qos = qos
	msg.TopicName = "abc"
	msg.Payload = []byte("abc")
	return msg
}

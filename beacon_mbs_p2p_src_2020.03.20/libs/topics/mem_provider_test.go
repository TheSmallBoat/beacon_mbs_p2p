package topics

import (
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/stretchr/testify/require"
)

func TestNextTopicLevelSuccess(t *testing.T) {
	topics := [][]byte{
		[]byte("sport/tennis/player1/#"),
		[]byte("sport/tennis/player1/ranking"),
		[]byte("sport/#"),
		[]byte("#"),
		[]byte("sport/tennis/#"),
		[]byte("+"),
		[]byte("+/tennis/#"),
		[]byte("sport/+/player1"),
		[]byte("/finance"),
	}

	levels := [][][]byte{
		{[]byte("sport"), []byte("tennis"), []byte("player1"), []byte("#")},
		{[]byte("sport"), []byte("tennis"), []byte("player1"), []byte("ranking")},
		{[]byte("sport"), []byte("#")},
		{[]byte("#")},
		{[]byte("sport"), []byte("tennis"), []byte("#")},
		{[]byte("+")},
		{[]byte("+"), []byte("tennis"), []byte("#")},
		{[]byte("sport"), []byte("+"), []byte("player1")},
		{[]byte("+"), []byte("finance")},
	}

	for i, topic := range topics {
		var (
			tl  []byte
			rem = topic
			err error
		)

		for _, level := range levels[i] {
			tl, rem, err = nextTopicLevel(rem)
			require.NoError(t, err)
			require.Equal(t, level, tl)
		}
	}
}

func TestNextTopicLevelFailure(t *testing.T) {
	topics := [][]byte{
		[]byte("sport/tennis#"),
		[]byte("sport/tennis/#/ranking"),
		[]byte("sport+"),
	}

	var (
		rem []byte
		err error
	)

	_, rem, err = nextTopicLevel(topics[0])
	require.NoError(t, err)

	_, rem, err = nextTopicLevel(rem)
	require.Error(t, err)

	_, rem, err = nextTopicLevel(topics[1])
	require.NoError(t, err)

	_, rem, err = nextTopicLevel(rem)
	require.NoError(t, err)

	_, rem, err = nextTopicLevel(rem)
	require.Error(t, err)

	_, rem, err = nextTopicLevel(topics[2])
	require.Error(t, err)
}

func TestSubscribeNodeInsert1(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("sport/tennis/player1/#")

	err := n.subscriberInsert(topic, 1, "sub1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.subscribeNodesMap))
	require.Equal(t, 0, len(n.subList))

	n2, ok := n.subscribeNodesMap["sport"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.subscribeNodesMap))
	require.Equal(t, 0, len(n2.subList))

	n3, ok := n2.subscribeNodesMap["tennis"]
	require.True(t, ok)
	require.Equal(t, 1, len(n3.subscribeNodesMap))
	require.Equal(t, 0, len(n3.subList))

	n4, ok := n3.subscribeNodesMap["player1"]
	require.True(t, ok)
	require.Equal(t, 1, len(n4.subscribeNodesMap))
	require.Equal(t, 0, len(n4.subList))

	n5, ok := n4.subscribeNodesMap["#"]
	require.True(t, ok)
	require.Equal(t, 0, len(n5.subscribeNodesMap))
	require.Equal(t, 1, len(n5.subList))
	require.Equal(t, "sub1", n5.subList[0].(string))
}

func TestSubscribeNodeInsert2(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("#")

	err := n.subscriberInsert(topic, 1, "sub1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.subscribeNodesMap))
	require.Equal(t, 0, len(n.subList))

	n2, ok := n.subscribeNodesMap["#"]
	require.True(t, ok)
	require.Equal(t, 0, len(n2.subscribeNodesMap))
	require.Equal(t, 1, len(n2.subList))
	require.Equal(t, "sub1", n2.subList[0].(string))
}

func TestSubscribeNodeInsert3(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("+/tennis/#")

	err := n.subscriberInsert(topic, 1, "sub1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.subscribeNodesMap))
	require.Equal(t, 0, len(n.subList))

	n2, ok := n.subscribeNodesMap["+"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.subscribeNodesMap))
	require.Equal(t, 0, len(n2.subList))

	n3, ok := n2.subscribeNodesMap["tennis"]
	require.True(t, ok)
	require.Equal(t, 1, len(n3.subscribeNodesMap))
	require.Equal(t, 0, len(n3.subList))

	n4, ok := n3.subscribeNodesMap["#"]
	require.True(t, ok)
	require.Equal(t, 0, len(n4.subscribeNodesMap))
	require.Equal(t, 1, len(n4.subList))
	require.Equal(t, "sub1", n4.subList[0].(string))
}

func TestSubscribeNodeInsert4(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("/finance")

	err := n.subscriberInsert(topic, 1, "sub1")

	require.NoError(t, err)
	require.Equal(t, 1, len(n.subscribeNodesMap))
	require.Equal(t, 0, len(n.subList))
	n2, ok := n.subscribeNodesMap["+"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.subscribeNodesMap))
	require.Equal(t, 0, len(n2.subList))

	n3, ok := n2.subscribeNodesMap["finance"]
	require.True(t, ok)
	require.Equal(t, 0, len(n3.subscribeNodesMap))
	require.Equal(t, 1, len(n3.subList))
	require.Equal(t, "sub1", n3.subList[0].(string))
}

func TestSubscribeNodeInsertDup(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("/finance")

	err := n.subscriberInsert(topic, 1, "sub1")
	err = n.subscriberInsert(topic, 1, "sub1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.subscribeNodesMap))
	require.Equal(t, 0, len(n.subList))

	n2, ok := n.subscribeNodesMap["+"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.subscribeNodesMap))
	require.Equal(t, 0, len(n2.subList))

	n3, ok := n2.subscribeNodesMap["finance"]
	require.True(t, ok)
	require.Equal(t, 0, len(n3.subscribeNodesMap))
	require.Equal(t, 1, len(n3.subList))
	require.Equal(t, "sub1", n3.subList[0].(string))
}

func TestSubscribeNodeRemove1(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("sport/tennis/player1/#")

	require.NoError(t, n.subscriberInsert(topic, 1, "sub1"))
	err := n.subscriberRemove([]byte("sport/tennis/player1/#"), "sub1")
	require.NoError(t, err)
	require.Equal(t, 0, len(n.subscribeNodesMap))
	require.Equal(t, 0, len(n.subList))
}

func TestSubscribeNodeRemove2(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("sport/tennis/player1/#")

	require.NoError(t, n.subscriberInsert(topic, 1, "sub1"))
	err := n.subscriberRemove([]byte("sport/tennis/player1"), "sub1")
	require.Error(t, err)
}

func TestSubscribeNodeRemove3(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("sport/tennis/player1/#")

	require.NoError(t, n.subscriberInsert(topic, 1, "sub1"))
	require.NoError(t, n.subscriberInsert(topic, 1, "sub2"))
	err := n.subscriberRemove([]byte("sport/tennis/player1/#"), nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(n.subscribeNodesMap))
	require.Equal(t, 0, len(n.subList))
}

func TestSubscribeNodeMatch1(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("sport/tennis/player1/#")
	require.NoError(t, n.subscriberInsert(topic, 1, "sub1"))

	subList := make([]interface{}, 0, 5)
	qosList := make([]byte, 0, 5)

	err := n.subscriberMatch([]byte("sport/tennis/player1/angel"), 1, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 1, len(subList))
	require.Equal(t, 1, int(qosList[0]))
}

func TestSubscribeNodeMatch2(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("sport/tennis/player1/#")
	require.NoError(t, n.subscriberInsert(topic, 1, "sub1"))

	subList := make([]interface{}, 0, 5)
	qosList := make([]byte, 0, 5)

	err := n.subscriberMatch([]byte("sport/tennis/player1/angel"), 1, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 1, len(subList))
	require.Equal(t, 1, int(qosList[0]))
}

func TestSubscribeNodeMatch3(t *testing.T) {
	n := newSubscribeNode()
	topic := []byte("sport/tennis/player1/#")
	require.NoError(t, n.subscriberInsert(topic, 2, "sub1"))

	subList := make([]interface{}, 0, 5)
	qosList := make([]byte, 0, 5)

	err := n.subscriberMatch([]byte("sport/tennis/player1/angel"), 2, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 1, len(subList))
	require.Equal(t, 2, int(qosList[0]))
}

func TestSubscribeNodeMatch4(t *testing.T) {
	n := newSubscribeNode()
	require.NoError(t, n.subscriberInsert([]byte("sport/tennis/#"), 2, "sub1"))

	subList := make([]interface{}, 0, 5)
	qosList := make([]byte, 0, 5)

	err := n.subscriberMatch([]byte("sport/tennis/player1/angel"), 2, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 1, len(subList))
	require.Equal(t, 2, int(qosList[0]))
}

func TestSubscribeNodeMatch5(t *testing.T) {
	n := newSubscribeNode()
	require.NoError(t, n.subscriberInsert([]byte("sport/tennis/+/angel"), 1, "sub1"))
	require.NoError(t, n.subscriberInsert([]byte("sport/tennis/player1/angel"), 1, "sub2"))

	subList := make([]interface{}, 0, 5)
	qosList := make([]byte, 0, 5)

	err := n.subscriberMatch([]byte("sport/tennis/player1/angel"), 1, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 2, len(subList))
}

func TestSubscribeNodeMatch6(t *testing.T) {
	n := newSubscribeNode()
	require.NoError(t, n.subscriberInsert([]byte("sport/tennis/#"), 2, "sub1"))
	require.NoError(t, n.subscriberInsert([]byte("sport/tennis"), 1, "sub2"))

	subList := make([]interface{}, 0, 5)
	qosList := make([]byte, 0, 5)

	err := n.subscriberMatch([]byte("sport/tennis/player1/angel"), 2, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 1, len(subList))
	require.Equal(t, "sub1", subList[0])
}

func TestSubscribeNodeMatch7(t *testing.T) {
	n := newSubscribeNode()
	require.NoError(t, n.subscriberInsert([]byte("+/+"), 2, "sub1"))

	subList := make([]interface{}, 0, 5)
	qosList := make([]byte, 0, 5)

	err := n.subscriberMatch([]byte("/finance"), 1, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 1, len(subList))
}

func TestSubscribeNodeMatch8(t *testing.T) {
	n := newSubscribeNode()
	require.NoError(t, n.subscriberInsert([]byte("/+"), 2, "sub1"))

	subList := make([]interface{}, 0, 5)
	qosList := make([]byte, 0, 5)

	err := n.subscriberMatch([]byte("/finance"), 1, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 1, len(subList))
}

func TestSubscribeNodeMatch9(t *testing.T) {
	n := newSubscribeNode()
	require.NoError(t, n.subscriberInsert([]byte("+"), 2, "sub1"))

	subList := make([]interface{}, 0, 5)
	qosList := make([]byte, 0, 5)

	err := n.subscriberMatch([]byte("/finance"), 1, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 0, len(subList))
}

func TestRetainNodeInsertRemove(t *testing.T) {
	n := newRetainNode()

	// --- Insert msg1
	msg := newPublishMessageLarge([]byte("sport/tennis/player1/ricardo"), 1)
	err := n.retainInsert([]byte(msg.TopicName), msg)

	require.NoError(t, err)
	require.Equal(t, 1, len(n.retainNodesMap))
	require.Nil(t, n.message)

	n2, ok := n.retainNodesMap["sport"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.retainNodesMap))
	require.Nil(t, n2.message)

	n3, ok := n2.retainNodesMap["tennis"]
	require.True(t, ok)
	require.Equal(t, 1, len(n3.retainNodesMap))
	require.Nil(t, n3.message)

	n4, ok := n3.retainNodesMap["player1"]
	require.True(t, ok)
	require.Equal(t, 1, len(n4.retainNodesMap))
	require.Nil(t, n4.message)

	n5, ok := n4.retainNodesMap["ricardo"]
	require.True(t, ok)
	require.Equal(t, 0, len(n5.retainNodesMap))
	require.NotNil(t, n5.message)
	require.Equal(t, msg.Qos, n5.message.Qos)
	require.Equal(t, msg.TopicName, n5.message.TopicName)
	require.Equal(t, msg.Payload, n5.message.Payload)

	// --- Insert msg2
	msg2 := newPublishMessageLarge([]byte("sport/tennis/player1/andre"), 1)
	err = n.retainInsert([]byte(msg2.TopicName), msg2)
	require.NoError(t, err)
	require.Equal(t, 2, len(n4.retainNodesMap))

	n6, ok := n4.retainNodesMap["andre"]
	require.True(t, ok)
	require.Equal(t, 0, len(n6.retainNodesMap))
	require.NotNil(t, n6.message)
	require.Equal(t, msg2.Qos, n6.message.Qos)
	require.Equal(t, msg2.TopicName, n6.message.TopicName)

	// --- Remove
	err = n.retainRemove([]byte("sport/tennis/player1/andre"))
	require.NoError(t, err)
	require.Equal(t, 1, len(n4.retainNodesMap))
}

func TestRetainNodeMatch(t *testing.T) {
	n := newRetainNode()

	msg1 := newPublishMessageLarge([]byte("sport/tennis/ricardo/stats"), 1)
	err := n.retainInsert([]byte(msg1.TopicName), msg1)
	require.NoError(t, err)

	msg2 := newPublishMessageLarge([]byte("sport/tennis/andre/stats"), 1)
	err = n.retainInsert([]byte(msg2.TopicName), msg2)
	require.NoError(t, err)

	msg3 := newPublishMessageLarge([]byte("sport/tennis/andre/bio"), 1)
	err = n.retainInsert([]byte(msg3.TopicName), msg3)
	require.NoError(t, err)

	var msglist []*packets.PublishPacket
	// ---
	err = n.retainMatch([]byte(msg1.TopicName), &msglist)
	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---
	msglist = msglist[0:0]
	err = n.retainMatch([]byte(msg2.TopicName), &msglist)
	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---
	msglist = msglist[0:0]
	err = n.retainMatch([]byte(msg3.TopicName), &msglist)
	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---
	msglist = msglist[0:0]
	err = n.retainMatch([]byte("sport/tennis/andre/+"), &msglist)
	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---
	msglist = msglist[0:0]
	err = n.retainMatch([]byte("sport/tennis/andre/#"), &msglist)
	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---
	msglist = msglist[0:0]
	err = n.retainMatch([]byte("sport/tennis/+/stats"), &msglist)
	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---
	msglist = msglist[0:0]
	err = n.retainMatch([]byte("sport/tennis/#"), &msglist)
	require.NoError(t, err)
	require.Equal(t, 3, len(msglist))
}

func TestMemTopicsSubscription(t *testing.T) {
	//Unregister("mem")
	//p := NewMemProvider()
	//Register("mem", p)
	UnRegisterMemTopicsProvider()
	RegisterMemTopicsProvider()

	mgr, err := NewManager("mem")
	require.NoError(t, err)

	qos, err := mgr.Subscribe([]byte("sports/tennis/+/stats"), 3, "sub1")
	require.Error(t, err)
	require.Equal(t, QosFailure, int(qos))

	qos, err = mgr.Subscribe([]byte("sports/tennis/+/stats"), QosExactlyOnce, nil)
	require.Error(t, err)
	require.Equal(t, QosFailure, int(qos))

	qos, err = mgr.Subscribe([]byte("sports/tennis/+/stats"), QosExactlyOnce, "sub1")
	require.NoError(t, err)
	require.Equal(t, 2, int(qos))

	err = mgr.Unsubscribe([]byte("sports/tennis"), "sub1")
	require.Error(t, err)

	subList := make([]interface{}, 5)
	qosList := make([]byte, 5)

	err = mgr.Subscribers([]byte("sports/tennis/angle/stats"), QosExactlyOnce, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 1, len(subList))
	require.Equal(t, 2, int(qosList[0]))

	err = mgr.Subscribers([]byte("sports/tennis/angle/stats"), QosAtLeastOnce, &subList, &qosList)
	require.NoError(t, err)
	require.Equal(t, 1, len(subList))
	require.Equal(t, 1, int(qosList[0]))

	err = mgr.Unsubscribe([]byte("sports/tennis/+/stats"), "sub1")
	require.NoError(t, err)
}

func TestMemTopicsRetained(t *testing.T) {
	//Unregister("mem")
	//p := NewMemProvider()
	//Register("mem", p)
	UnRegisterMemTopicsProvider()
	RegisterMemTopicsProvider()

	mgr, err := NewManager("mem")
	require.NoError(t, err)
	require.NotNil(t, mgr)

	msg1 := newPublishMessageLarge([]byte("sport/tennis/ricardo/stats"), 1)
	err = mgr.Retain(msg1)
	require.NoError(t, err)

	msg2 := newPublishMessageLarge([]byte("sport/tennis/andre/stats"), 1)
	err = mgr.Retain(msg2)
	require.NoError(t, err)

	msg3 := newPublishMessageLarge([]byte("sport/tennis/andre/bio"), 1)
	err = mgr.Retain(msg3)
	require.NoError(t, err)

	var msgList []*packets.PublishPacket

	// ---
	err = mgr.Retained([]byte(msg1.TopicName), &msgList)
	require.NoError(t, err)
	require.Equal(t, 1, len(msgList))

	// ---
	msgList = msgList[0:0]
	err = mgr.Retained([]byte(msg2.TopicName), &msgList)
	require.NoError(t, err)
	require.Equal(t, 1, len(msgList))

	// ---
	msgList = msgList[0:0]
	err = mgr.Retained([]byte(msg3.TopicName), &msgList)
	require.NoError(t, err)
	require.Equal(t, 1, len(msgList))

	// ---
	msgList = msgList[0:0]
	err = mgr.Retained([]byte("sport/tennis/andre/+"), &msgList)
	require.NoError(t, err)
	require.Equal(t, 2, len(msgList))

	// ---
	msgList = msgList[0:0]
	err = mgr.Retained([]byte("sport/tennis/andre/#"), &msgList)
	require.NoError(t, err)
	require.Equal(t, 2, len(msgList))

	// ---
	msgList = msgList[0:0]
	err = mgr.Retained([]byte("sport/tennis/+/stats"), &msgList)
	require.NoError(t, err)
	require.Equal(t, 2, len(msgList))

	// ---
	msgList = msgList[0:0]
	err = mgr.Retained([]byte("sport/tennis/#"), &msgList)
	require.NoError(t, err)
	require.Equal(t, 3, len(msgList))
}

func newPublishMessageLarge(topic []byte, qos byte) *packets.PublishPacket {
	msg := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	msg.Qos = qos
	msg.TopicName = string(topic)
	msg.Payload = make([]byte, 1024*1024)
	return msg
}

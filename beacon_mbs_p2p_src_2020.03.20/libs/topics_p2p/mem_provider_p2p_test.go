package topics_p2p

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubscribeNode4P2PInsert1(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("sport/tennis/player1/#")

	err := n.subscriber4P2PInsert(topic, "broker1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n.brokerList))

	n2, ok := n.subscribeNodes4P2PMap["sport"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n2.brokerList))

	n3, ok := n2.subscribeNodes4P2PMap["tennis"]
	require.True(t, ok)
	require.Equal(t, 1, len(n3.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n3.brokerList))

	n4, ok := n3.subscribeNodes4P2PMap["player1"]
	require.True(t, ok)
	require.Equal(t, 1, len(n4.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n4.brokerList))

	n5, ok := n4.subscribeNodes4P2PMap["#"]
	require.True(t, ok)
	require.Equal(t, 0, len(n5.subscribeNodes4P2PMap))
	require.Equal(t, 1, len(n5.brokerList))
	require.Equal(t, "broker1", n5.brokerList[0].(string))
}

func TestSubscribeNode4P2PInsert2(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("#")

	err := n.subscriber4P2PInsert(topic, "broker1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n.brokerList))

	n2, ok := n.subscribeNodes4P2PMap["#"]
	require.True(t, ok)
	require.Equal(t, 0, len(n2.subscribeNodes4P2PMap))
	require.Equal(t, 1, len(n2.brokerList))
	require.Equal(t, "broker1", n2.brokerList[0].(string))
}

func TestSubscribeNode4P2PInsert3(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("+/tennis/#")

	err := n.subscriber4P2PInsert(topic, "broker1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n.brokerList))

	n2, ok := n.subscribeNodes4P2PMap["+"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n2.brokerList))

	n3, ok := n2.subscribeNodes4P2PMap["tennis"]
	require.True(t, ok)
	require.Equal(t, 1, len(n3.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n3.brokerList))

	n4, ok := n3.subscribeNodes4P2PMap["#"]
	require.True(t, ok)
	require.Equal(t, 0, len(n4.subscribeNodes4P2PMap))
	require.Equal(t, 1, len(n4.brokerList))
	require.Equal(t, "broker1", n4.brokerList[0].(string))
}

func TestSubscribeNode4P2PInsert4(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("/finance")

	err := n.subscriber4P2PInsert(topic, "broker1")

	require.NoError(t, err)
	require.Equal(t, 1, len(n.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n.brokerList))
	n2, ok := n.subscribeNodes4P2PMap["+"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n2.brokerList))

	n3, ok := n2.subscribeNodes4P2PMap["finance"]
	require.True(t, ok)
	require.Equal(t, 0, len(n3.subscribeNodes4P2PMap))
	require.Equal(t, 1, len(n3.brokerList))
	require.Equal(t, "broker1", n3.brokerList[0].(string))
}

func TestSubscribeNode4P2PInsertDup(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("/finance")

	err := n.subscriber4P2PInsert(topic, "broker1")
	err = n.subscriber4P2PInsert(topic, "broker1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n.brokerList))

	n2, ok := n.subscribeNodes4P2PMap["+"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n2.brokerList))

	n3, ok := n2.subscribeNodes4P2PMap["finance"]
	require.True(t, ok)
	require.Equal(t, 0, len(n3.subscribeNodes4P2PMap))
	require.Equal(t, 1, len(n3.brokerList))
	require.Equal(t, "broker1", n3.brokerList[0].(string))
}

func TestSubscribeNode4P2PRemove1(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("sport/tennis/player1/#")

	require.NoError(t, n.subscriber4P2PInsert(topic, "broker1"))
	err := n.subscriber4P2PRemove([]byte("sport/tennis/player1/#"), "broker1")
	require.NoError(t, err)
	require.Equal(t, 0, len(n.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n.brokerList))
}

func TestSubscribeNode4P2PRemove2(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("sport/tennis/player1/#")

	require.NoError(t, n.subscriber4P2PInsert(topic, "broker1"))
	err := n.subscriber4P2PRemove([]byte("sport/tennis/player1"), "broker1")
	require.Error(t, err)
}

func TestSubscribeNode4P2PRemove3(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("sport/tennis/player1/#")

	require.NoError(t, n.subscriber4P2PInsert(topic, "broker1"))
	require.NoError(t, n.subscriber4P2PInsert(topic, "broker2"))
	err := n.subscriber4P2PRemove([]byte("sport/tennis/player1/#"), nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(n.subscribeNodes4P2PMap))
	require.Equal(t, 0, len(n.brokerList))
}

func TestSubscribeNode4P2PMatch1(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("sport/tennis/player1/#")
	require.NoError(t, n.subscriber4P2PInsert(topic, "broker1"))

	brokerList := make([]interface{}, 0, 5)

	err := n.subscriber4P2PMatch([]byte("sport/tennis/player1/angel"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 1, len(brokerList))
}

func TestSubscribeNode4P2PMatch2(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("sport/tennis/player1/#")
	require.NoError(t, n.subscriber4P2PInsert(topic, "broker1"))

	brokerList := make([]interface{}, 0, 5)

	err := n.subscriber4P2PMatch([]byte("sport/tennis/player1/angel"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 1, len(brokerList))
}

func TestSubscribeNode4P2PMatch3(t *testing.T) {
	n := newSubscribeNode4P2P()
	topic := []byte("sport/tennis/player1/#")
	require.NoError(t, n.subscriber4P2PInsert(topic, "broker1"))

	brokerList := make([]interface{}, 0, 5)

	err := n.subscriber4P2PMatch([]byte("sport/tennis/player1/angel"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 1, len(brokerList))
}

func TestSubscribeNode4P2PMatch4(t *testing.T) {
	n := newSubscribeNode4P2P()
	require.NoError(t, n.subscriber4P2PInsert([]byte("sport/tennis/#"), "broker1"))

	brokerList := make([]interface{}, 0, 5)

	err := n.subscriber4P2PMatch([]byte("sport/tennis/player1/angel"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 1, len(brokerList))
}

func TestSubscribeNode4P2PMatch5(t *testing.T) {
	n := newSubscribeNode4P2P()
	require.NoError(t, n.subscriber4P2PInsert([]byte("sport/tennis/+/angel"), "broker1"))
	require.NoError(t, n.subscriber4P2PInsert([]byte("sport/tennis/player1/angel"), "broker1"))

	brokerList := make([]interface{}, 0, 5)

	err := n.subscriber4P2PMatch([]byte("sport/tennis/player1/angel"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 2, len(brokerList))
}

func TestSubscribeNode4P2PMatch6(t *testing.T) {
	n := newSubscribeNode4P2P()
	require.NoError(t, n.subscriber4P2PInsert([]byte("sport/tennis/#"), "broker1"))
	require.NoError(t, n.subscriber4P2PInsert([]byte("sport/tennis"), "broker1"))

	brokerList := make([]interface{}, 0, 5)

	err := n.subscriber4P2PMatch([]byte("sport/tennis/player1/angel"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 1, len(brokerList))
	require.Equal(t, "broker1", brokerList[0])
}

func TestSubscribeNode4P2PMatch7(t *testing.T) {
	n := newSubscribeNode4P2P()
	require.NoError(t, n.subscriber4P2PInsert([]byte("+/+"), "broker1"))

	brokerList := make([]interface{}, 0, 5)

	err := n.subscriber4P2PMatch([]byte("/finance"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 1, len(brokerList))
}

func TestSubscribeNode4P2PMatch8(t *testing.T) {
	n := newSubscribeNode4P2P()
	require.NoError(t, n.subscriber4P2PInsert([]byte("/+"), "broker1"))

	brokerList := make([]interface{}, 0, 5)

	err := n.subscriber4P2PMatch([]byte("/finance"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 1, len(brokerList))
}

func TestSubscribeNode4P2PMatch9(t *testing.T) {
	n := newSubscribeNode4P2P()
	require.NoError(t, n.subscriber4P2PInsert([]byte("+"), "broker1"))

	brokerList := make([]interface{}, 0, 5)

	err := n.subscriber4P2PMatch([]byte("/finance"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 0, len(brokerList))
}

func TestMemTopicsSubscription4P2P(t *testing.T) {
	//Unregister("mem")
	//p := NewMemProvider()
	//Register("mem", p)
	UnRegisterMemTopicsProvider4P2P()
	RegisterMemTopicsProvider4P2P()

	mgr, err := NewManager4P2P("mem")
	require.NoError(t, err)

	err = mgr.Subscribe4P2P([]byte("sports/tennis/+/stats"), "broker1")
	require.NoError(t, err)

	err = mgr.Subscribe4P2P([]byte("sports/tennis/+/stats"), nil)
	require.Error(t, err)

	err = mgr.Unsubscribe4P2P([]byte("sports/tennis"), "broker1")
	require.Error(t, err)

	brokerList := make([]interface{}, 5)

	err = mgr.Brokers4P2P([]byte("sports/tennis/+/stats"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 1, len(brokerList))

	err = mgr.Brokers4P2P([]byte("sports/tennis/angle/stats"), &brokerList)
	require.NoError(t, err)
	require.Equal(t, 1, len(brokerList))

	err = mgr.Unsubscribe4P2P([]byte("sports/tennis/+/stats"), "broker1")
	require.NoError(t, err)
}

func TestMemSimpleTopicsAction4P2P(t *testing.T) {
	UnRegisterMemTopicsProvider4P2P()
	RegisterMemTopicsProvider4P2P()

	mgr, err := NewManager4P2P("mem")
	require.NoError(t, err)

	action1 := SimpleTopicAction{Subscribe4P2POperateCode, []byte("sports/tennis/+/stats"), "broker2"}
	err = mgr.BrokerTopics4P2PSimpleAction(action1)
	require.NoError(t, err)

	action2 := SimpleTopicAction{Brokers4P2PCheckOperateCode, []byte("sports/tennis/+/stats"), "broker2"}
	err = mgr.BrokerTopics4P2PSimpleAction(action2)
	require.NoError(t, err)

	action3 := SimpleTopicAction{UnSubscribe4P2POperateCode, []byte("sports/tennis"), "broker2"}
	err = mgr.BrokerTopics4P2PSimpleAction(action3)
	require.Error(t, err)

	action4 := SimpleTopicAction{UnSubscribe4P2POperateCode, []byte("sports/tennis/+/stats"), "broker2"}
	err = mgr.BrokerTopics4P2PSimpleAction(action4)
	require.NoError(t, err)

	action5 := SimpleTopicAction{Brokers4P2PCheckOperateCode, []byte("sports/tennis"), "broker2"}
	json5, errJ5 := action5.Marshal()
	require.NoError(t, errJ5)
	err = mgr.BrokerTopics4P2PSimpleAction(action5)
	require.Error(t, err)

	action6 := SimpleTopicAction{Subscribe4P2POperateCode, []byte("sports/tennis"), "broker2"}
	data6, errD := mgr.BrokerTopics4P2PSimpleActionToJSON(action6)
	json6, errJ6 := action6.Marshal()
	require.NoError(t, errD)
	require.NoError(t, errJ6)
	require.EqualValues(t, json6, data6)

	err = mgr.BrokerTopics4P2PSimpleActionFromJSON(json5)
	require.NoError(t, err)

	err = mgr.BrokerTopics4P2PSimpleAction(action5)
	require.NoError(t, err)
}

func TestMemTopicsActions4P2P(t *testing.T) {
	UnRegisterMemTopicsProvider4P2P()
	RegisterMemTopicsProvider4P2P()

	mgr, err := NewManager4P2P("mem")
	require.NoError(t, err)

	aE1 := ActionElement{Subscribe4P2POperateCode, []byte("sports/tennis")}
	aE2 := ActionElement{Brokers4P2PCheckOperateCode, []byte("sports/tennis")}
	aE3 := ActionElement{UnSubscribe4P2POperateCode, []byte("sports/tennis")}

	aeList := make([]ActionElement, 0, 5)
	aeList = append(aeList, aE1, aE2, aE3)

	tas := TopicActions{hex.EncodeToString([]byte("broker3")), aeList}
	dataD, errD := mgr.BrokerTopics4P2PActionsToJSON(tas)
	require.NoError(t, errD)
	jsonD, errJ := tas.Marshal()
	require.NoError(t, errJ)
	require.EqualValues(t, jsonD, dataD)

	err = mgr.BrokerTopics4P2PActionsFromJSON(jsonD)
	require.NoError(t, err)

	err = mgr.BrokerTopics4P2PActions(tas)
	require.NoError(t, err)
}

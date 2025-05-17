// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: werbenhu

package json

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/mochi-mqtt/server/v2/system"
)

const (
	// defaultJSONFile is the default file path for the JSON data file.
	defaultJSONFile = "data.json"
)

// clientKey returns a primary key for a client.
func clientKey(cl *mqtt.Client) string {
	return storage.ClientKey + "_" + cl.ID
}

// subscriptionKey returns a primary key for a subscription.
func subscriptionKey(cl *mqtt.Client, filter string) string {
	return storage.SubscriptionKey + "_" + cl.ID + ":" + filter
}

// retainedKey returns a primary key for a retained message.
func retainedKey(topic string) string {
	return storage.RetainedKey + "_" + topic
}

// inflightKey returns a primary key for an inflight message.
func inflightKey(cl *mqtt.Client, pk packets.Packet) string {
	return storage.InflightKey + "_" + cl.ID + ":" + pk.FormatID()
}

// sysInfoKey returns a primary key for system info.
func sysInfoKey() string {
	return storage.SysInfoKey
}

// Options contains configuration settings for the JSON storage.
type Options struct {
	Path string `yaml:"path" json:"path"`
}

// jsonData is a container for all data to be stored in the JSON file.
type jsonData struct {
	Clients       map[string]storage.Client       `json:"clients"`
	Subscriptions map[string]storage.Subscription `json:"subscriptions"`
	Retained      map[string]storage.Message      `json:"retained"`
	Inflight      map[string]storage.Message      `json:"inflight"`
	SysInfo       storage.SystemInfo              `json:"sysinfo"`
}

// Hook is a persistent storage hook based using a JSON file store as a backend.
type Hook struct {
	mqtt.HookBase
	config *Options     // options for configuring the JSON storage.
	data   jsonData     // in-memory representation of the data
	mutex  sync.RWMutex // for thread-safe access to the data
}

// ID returns the id of the hook.
func (h *Hook) ID() string {
	return "json-storage" // ID of the hook
}

// Provides indicates which hook methods this hook provides.
func (h *Hook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnSessionEstablished,
		mqtt.OnDisconnect,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribed,
		mqtt.OnRetainMessage,
		mqtt.OnWillSent,
		mqtt.OnQosPublish,
		mqtt.OnQosComplete,
		mqtt.OnQosDropped,
		mqtt.OnSysInfoTick,
		mqtt.OnClientExpired,
		mqtt.OnRetainedExpired,
		mqtt.StoredClients,
		mqtt.StoredInflightMessages,
		mqtt.StoredRetainedMessages,
		mqtt.StoredSubscriptions,
		mqtt.StoredSysInfo,
	}, []byte{b})
}

// Init initializes and loads data from the JSON file.
func (h *Hook) Init(config any) error {
	if _, ok := config.(*Options); !ok && config != nil {
		return mqtt.ErrInvalidConfigType
	}

	if config == nil {
		h.config = new(Options)
	} else {
		h.config = config.(*Options)
	}

	if len(h.config.Path) == 0 {
		h.config.Path = defaultJSONFile
	}

	h.data = jsonData{
		Clients:       make(map[string]storage.Client),
		Subscriptions: make(map[string]storage.Subscription),
		Retained:      make(map[string]storage.Message),
		Inflight:      make(map[string]storage.Message),
	}

	return h.loadData()
}

// loadData reads the data from the JSON file into memory.
func (h *Hook) loadData() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	file, err := os.ReadFile(h.config.Path)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, initialize with empty data (already done in Init)
			// and attempt to save it to create the file.
			return h.saveDataLocked()
		}
		return fmt.Errorf("failed to read data file: %w", err)
	}

	if len(file) == 0 { // Empty file, treat as new
		return h.saveDataLocked()
	}

	err = json.Unmarshal(file, &h.data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}

	// Ensure maps are not nil after unmarshaling an empty or partial JSON
	if h.data.Clients == nil {
		h.data.Clients = make(map[string]storage.Client)
	}
	if h.data.Subscriptions == nil {
		h.data.Subscriptions = make(map[string]storage.Subscription)
	}
	if h.data.Retained == nil {
		h.data.Retained = make(map[string]storage.Message)
	}
	if h.data.Inflight == nil {
		h.data.Inflight = make(map[string]storage.Message)
	}

	return nil
}

// saveDataLocked writes the current in-memory data to the JSON file. Assumes lock is held.
func (h *Hook) saveDataLocked() error {
	bytes, err := json.MarshalIndent(h.data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	err = os.WriteFile(h.config.Path, bytes, 0644)
	if err != nil {
		return fmt.Errorf("failed to write data file: %w", err)
	}
	return nil
}

// saveData is a wrapper for saveDataLocked with mutex handling.
func (h *Hook) saveData() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.saveDataLocked()
}

// Stop saves the data to the JSON file.
func (h *Hook) Stop() error {
	return h.saveData()
}

// OnSessionEstablished adds a client to the store when their session is established.
func (h *Hook) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	h.updateClient(cl)
}

// OnWillSent is called when a client sends a Will Message and the Will Message is removed from the client record.
func (h *Hook) OnWillSent(cl *mqtt.Client, pk packets.Packet) {
	h.updateClient(cl)
}

// updateClient writes the client data to the store.
func (h *Hook) updateClient(cl *mqtt.Client) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	props := cl.Properties.Props.Copy(false)
	in := storage.Client{
		ID:              cl.ID,
		T:               storage.ClientKey,
		Remote:          cl.Net.Remote,
		Listener:        cl.Net.Listener,
		Username:        cl.Properties.Username,
		Clean:           cl.Properties.Clean,
		ProtocolVersion: cl.Properties.ProtocolVersion,
		Properties: storage.ClientProperties{
			SessionExpiryInterval: props.SessionExpiryInterval,
			AuthenticationMethod:  props.AuthenticationMethod,
			AuthenticationData:    props.AuthenticationData,
			RequestProblemInfo:    props.RequestProblemInfo,
			RequestResponseInfo:   props.RequestResponseInfo,
			ReceiveMaximum:        props.ReceiveMaximum,
			TopicAliasMaximum:     props.TopicAliasMaximum,
			User:                  props.User,
			MaximumPacketSize:     props.MaximumPacketSize,
		},
		Will: storage.ClientWill(cl.Properties.Will),
	}
	h.data.Clients[clientKey(cl)] = in
	if err := h.saveDataLocked(); err != nil {
		h.Log.Error("failed to save client data", "error", err, "client", cl.ID)
	}
}

// OnDisconnect removes a client from the store if their session has expired.
func (h *Hook) OnDisconnect(cl *mqtt.Client, _ error, expire bool) {
	h.updateClient(cl) // Update client state first (e.g. LWT cleared)

	if !expire {
		return
	}

	if errors.Is(cl.StopCause(), packets.ErrSessionTakenOver) {
		return
	}

	h.mutex.Lock()
	defer h.mutex.Unlock()
	delete(h.data.Clients, clientKey(cl))
	if err := h.saveDataLocked(); err != nil {
		h.Log.Error("failed to save data after disconnecting client", "error", err, "client", cl.ID)
	}
}

// OnSubscribed adds one or more client subscriptions to the store.
func (h *Hook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	var in storage.Subscription
	for i := 0; i < len(pk.Filters); i++ {
		in = storage.Subscription{
			ID:                subscriptionKey(cl, pk.Filters[i].Filter),
			T:                 storage.SubscriptionKey,
			Client:            cl.ID,
			Qos:               reasonCodes[i],
			Filter:            pk.Filters[i].Filter,
			Identifier:        pk.Filters[i].Identifier,
			NoLocal:           pk.Filters[i].NoLocal,
			RetainHandling:    pk.Filters[i].RetainHandling,
			RetainAsPublished: pk.Filters[i].RetainAsPublished,
		}
		h.data.Subscriptions[in.ID] = in
	}
	if err := h.saveDataLocked(); err != nil {
		h.Log.Error("failed to save subscription data", "error", err, "client", cl.ID)
	}
}

// OnUnsubscribed removes one or more client subscriptions from the store.
func (h *Hook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for i := 0; i < len(pk.Filters); i++ {
		delete(h.data.Subscriptions, subscriptionKey(cl, pk.Filters[i].Filter))
	}
	if err := h.saveDataLocked(); err != nil {
		h.Log.Error("failed to save data after unsubscribing", "error", err, "client", cl.ID)
	}
}

// OnRetainMessage adds a retained message for a topic to the store.
func (h *Hook) OnRetainMessage(cl *mqtt.Client, pk packets.Packet, r int64) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	key := retainedKey(pk.TopicName)
	if r == -1 || len(pk.Payload) == 0 { // MQTTv5 spec: zero length payload means delete retained message
		delete(h.data.Retained, key)
	} else {
		props := pk.Properties.Copy(false)
		in := storage.Message{
			ID:          key,
			T:           storage.RetainedKey,
			FixedHeader: pk.FixedHeader,
			TopicName:   pk.TopicName,
			Payload:     pk.Payload,
			Created:     pk.Created,
			Client:      cl.ID,
			Origin:      pk.Origin,
			Properties: storage.MessageProperties{
				PayloadFormat:          props.PayloadFormat,
				MessageExpiryInterval:  props.MessageExpiryInterval,
				ContentType:            props.ContentType,
				ResponseTopic:          props.ResponseTopic,
				CorrelationData:        props.CorrelationData,
				SubscriptionIdentifier: props.SubscriptionIdentifier,
				TopicAlias:             props.TopicAlias,
				User:                   props.User,
			},
		}
		h.data.Retained[key] = in
	}

	if err := h.saveDataLocked(); err != nil {
		h.Log.Error("failed to save retained message data", "error", err, "topic", pk.TopicName)
	}
}

// OnQosPublish adds or updates an inflight message in the store.
func (h *Hook) OnQosPublish(cl *mqtt.Client, pk packets.Packet, sent int64, resends int) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	props := pk.Properties.Copy(false)
	in := storage.Message{
		ID:          inflightKey(cl, pk),
		T:           storage.InflightKey,
		Client:      cl.ID,
		Origin:      pk.Origin,
		PacketID:    pk.PacketID,
		FixedHeader: pk.FixedHeader,
		TopicName:   pk.TopicName,
		Payload:     pk.Payload,
		Sent:        sent,
		Created:     pk.Created,
		Properties: storage.MessageProperties{
			PayloadFormat:          props.PayloadFormat,
			MessageExpiryInterval:  props.MessageExpiryInterval,
			ContentType:            props.ContentType,
			ResponseTopic:          props.ResponseTopic,
			CorrelationData:        props.CorrelationData,
			SubscriptionIdentifier: props.SubscriptionIdentifier,
			TopicAlias:             props.TopicAlias,
			User:                   props.User,
		},
	}
	h.data.Inflight[in.ID] = in
	if err := h.saveDataLocked(); err != nil {
		h.Log.Error("failed to save inflight message data", "error", err, "client", cl.ID, "packetID", pk.PacketID)
	}
}

// OnQosComplete removes a resolved inflight message from the store.
func (h *Hook) OnQosComplete(cl *mqtt.Client, pk packets.Packet) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	delete(h.data.Inflight, inflightKey(cl, pk))
	if err := h.saveDataLocked(); err != nil {
		h.Log.Error("failed to save data after completing QOS", "error", err, "client", cl.ID, "packetID", pk.PacketID)
	}
}

// OnQosDropped removes a dropped inflight message from the store.
func (h *Hook) OnQosDropped(cl *mqtt.Client, pk packets.Packet) {
	h.OnQosComplete(cl, pk) // Same logic as OnQosComplete
}

// OnSysInfoTick stores the latest system info in the store.
func (h *Hook) OnSysInfoTick(sys *system.Info) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.data.SysInfo = storage.SystemInfo{
		ID:   sysInfoKey(),
		T:    storage.SysInfoKey,
		Info: *sys.Clone(),
	}
	if err := h.saveDataLocked(); err != nil {
		h.Log.Error("failed to save system info data", "error", err)
	}
}

// OnRetainedExpired deletes expired retained messages from the store.
func (h *Hook) OnRetainedExpired(filter string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	delete(h.data.Retained, retainedKey(filter))
	if err := h.saveDataLocked(); err != nil {
		h.Log.Error("failed to save data after expiring retained message", "error", err, "filter", filter)
	}
}

// OnClientExpired deleted expired clients from the store.
func (h *Hook) OnClientExpired(cl *mqtt.Client) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	delete(h.data.Clients, clientKey(cl))
	if err := h.saveDataLocked(); err != nil {
		h.Log.Error("failed to save data after expiring client", "error", err, "client", cl.ID)
	}
}

// StoredClients returns all stored clients from the store.
func (h *Hook) StoredClients() (v []storage.Client, err error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	for _, client := range h.data.Clients {
		v = append(v, client)
	}
	return v, nil
}

// StoredSubscriptions returns all stored subscriptions from the store.
func (h *Hook) StoredSubscriptions() (v []storage.Subscription, err error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	for _, sub := range h.data.Subscriptions {
		v = append(v, sub)
	}
	return v, nil
}

// StoredRetainedMessages returns all stored retained messages from the store.
func (h *Hook) StoredRetainedMessages() (v []storage.Message, err error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	for _, msg := range h.data.Retained {
		v = append(v, msg)
	}
	return v, nil
}

// StoredInflightMessages returns all stored inflight messages from the store.
func (h *Hook) StoredInflightMessages() (v []storage.Message, err error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	for _, msg := range h.data.Inflight {
		v = append(v, msg)
	}
	return v, nil
}

// StoredSysInfo returns the system info from the store.
func (h *Hook) StoredSysInfo() (v storage.SystemInfo, err error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.data.SysInfo, nil
}

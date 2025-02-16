package protocol

import (
	"bytes"

	"godis/interface/redis"
)

// PongReply is +PONG
type PongReply struct{}

var pongBytes = []byte("+PONG\r\n")
var pongDataString = "PONG"

// ToBytes marshal redis.Reply
func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

func (r *PongReply) DataString() string {
	return pongDataString
}

func MakePongReply() *PongReply {
	return &PongReply{}
}

// OkReply is +OK
type OkReply struct{}

var okBytes = []byte("+OK\r\n")
var okStatusDataString = "OK"

// ToBytes marshal redis.Reply
func (r *OkReply) ToBytes() []byte {
	return okBytes
}

func (r *OkReply) DataString() string {
	return okStatusDataString
}

var theOkReply = new(OkReply)

// MakeOkReply returns a ok protocol
func MakeOkReply() *OkReply {
	return theOkReply
}

var nullBulkBytes = []byte("$-1\r\n")

// NullBulkReply is empty string
type NullBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func (r *NullBulkReply) DataString() string {
	return "(nil)"
}

// MakeNullBulkReply creates a new NullBulkReply
func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

var emptyMultiBulkBytes = []byte("*0\r\n")

// EmptyMultiBulkReply is a empty list
type EmptyMultiBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}
func (r *EmptyMultiBulkReply) DataString() string {
	return "(empty list or set)"
}

// MakeEmptyMultiBulkReply creates EmptyMultiBulkReply
func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

func IsEmptyMultiBulkReply(reply redis.Reply) bool {
	return bytes.Equal(reply.ToBytes(), emptyMultiBulkBytes)
}

// NoReply respond nothing, for commands like subscribe
type NoReply struct{}

var noBytes = []byte("")

// ToBytes marshal redis.Reply
func (r *NoReply) ToBytes() []byte {
	return noBytes
}
func (r *NoReply) DataString() string {
	return ""
}

func MakeNoReply() *NoReply {
	return &NoReply{}
}

// QueuedReply is +QUEUED
type QueuedReply struct{}

var queuedBytes = []byte("+QUEUED\r\n")

// ToBytes marshal redis.Reply
func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}
func (r *QueuedReply) DataString() string {
	return "QUEUED"
}

var theQueuedReply = new(QueuedReply)

// MakeQueuedReply returns a QUEUED protocol
func MakeQueuedReply() *QueuedReply {
	return theQueuedReply
}

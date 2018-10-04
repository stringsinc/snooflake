// Package snooflake implements Snooflake, a distributed unique ID generator inspired by Twitter's Snowflake.
//
// A Snooflake ID is composed of
//     39 bits for time in units of 10 msec
//      8 bits for a sequence number
//     16 bits for a machine id
package snooflake

import (
	"errors"
	"net"
	"sync"
	"time"
)

// These constants are the bit lengths of Snooflake ID parts.
const (
	BitLenTime      = 39                               // bit length of time
	BitLenSequence  = 8                                // bit length of sequence number
	BitLenMachineID = 63 - BitLenTime - BitLenSequence // bit length of machine id
)

// Settings configures Snooflake:
//
// StartTime is the time since which the Snooflake time is defined as the elapsed time.
// If StartTime is 0, the start time of the Snooflake is set to "2014-09-01 00:00:00 +0000 UTC".
// If StartTime is ahead of the current time, Snooflake is not created.
//
// MachineID returns the unique ID of the Snooflake instance.
// If MachineID returns an error, Snooflake is not created.
// If MachineID is nil, default MachineID is used.
// Default MachineID returns the lower 16 bits of the private IP address.
//
// CheckMachineID validates the uniqueness of the machine ID.
// If CheckMachineID returns false, Snooflake is not created.
// If CheckMachineID is nil, no validation is done.
type Settings struct {
	StartTime      time.Time
	MachineID      func() (uint16, error)
	CheckMachineID func(uint16) bool
}

// Snooflake is a distributed unique ID generator.
type Snooflake struct {
	mutex       *sync.Mutex
	startTime   int64
	elapsedTime int64
	sequence    uint16
	machineID   uint16
}

// NewSnooflake returns a new Snooflake configured with the given Settings.
// NewSnooflake returns nil in the following cases:
// - Settings.StartTime is ahead of the current time.
// - Settings.MachineID returns an error.
// - Settings.CheckMachineID returns false.
func NewSnooflake(st Settings) *Snooflake {
	sf := new(Snooflake)
	sf.mutex = new(sync.Mutex)
	sf.sequence = uint16(1<<BitLenSequence - 1)

	if st.StartTime.After(time.Now()) {
		return nil
	}
	if st.StartTime.IsZero() {
		sf.startTime = toSnooflakeTime(time.Date(2014, 9, 1, 0, 0, 0, 0, time.UTC))
	} else {
		sf.startTime = toSnooflakeTime(st.StartTime)
	}

	var err error
	if st.MachineID == nil {
		sf.machineID, err = lower16BitPrivateIP()
	} else {
		sf.machineID, err = st.MachineID()
	}
	if err != nil || (st.CheckMachineID != nil && !st.CheckMachineID(sf.machineID)) {
		return nil
	}

	return sf
}

func (sf *Snooflake) NextIDs(num int) ([]uint64, error) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	ids := make([]uint64, num)
	for i := 0; i < num; i++ {
		id, err := sf.nextID()
		if err != nil {
			return ids, err
		}
		ids[i] = id
	}
	return ids, nil
}

// NextID generates a next unique ID.
// After the Snooflake time overflows, NextID returns an error.
func (sf *Snooflake) NextID() (uint64, error) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	return sf.nextID()
}

// Not thread safe
func (sf *Snooflake) nextID() (uint64, error) {
	const maskSequence = uint16(1<<BitLenSequence - 1)

	current := currentElapsedTime(sf.startTime)
	if sf.elapsedTime < current {
		sf.elapsedTime = current
		sf.sequence = 0
	} else { // sf.elapsedTime >= current
		sf.sequence = (sf.sequence + 1) & maskSequence
		if sf.sequence == 0 {
			sf.elapsedTime++
			overtime := sf.elapsedTime - current
			time.Sleep(sleepTime((overtime)))
		}
	}

	return sf.toID()
}

const snooflakeTimeUnit = 1e6 // 1 msec

func toSnooflakeTime(t time.Time) int64 {
	return t.UTC().UnixNano() / snooflakeTimeUnit
}

func currentElapsedTime(startTime int64) int64 {
	return toSnooflakeTime(time.Now()) - startTime
}

func sleepTime(overtime int64) time.Duration {
	return time.Duration(overtime)*1*time.Millisecond -
		time.Duration(time.Now().UTC().UnixNano()%snooflakeTimeUnit)*time.Nanosecond
}

func (sf *Snooflake) toID() (uint64, error) {
	if sf.elapsedTime >= 1<<BitLenTime {
		return 0, errors.New("over the time limit")
	}

	return uint64(sf.elapsedTime)<<(BitLenSequence+BitLenMachineID) |
		uint64(sf.sequence)<<BitLenMachineID |
		uint64(sf.machineID), nil
}

func privateIPv4() (net.IP, error) {
	as, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range as {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}

		ip := ipnet.IP.To4()
		if isPrivateIPv4(ip) {
			return ip, nil
		}
	}
	return nil, errors.New("no private ip address")
}

func isPrivateIPv4(ip net.IP) bool {
	return ip != nil &&
		(ip[0] == 10 || ip[0] == 172 && (ip[1] >= 16 && ip[1] < 32) || ip[0] == 192 && ip[1] == 168)
}

func lower16BitPrivateIP() (uint16, error) {
	ip, err := privateIPv4()
	if err != nil {
		return 0, err
	}

	return uint16(ip[2])<<8 + uint16(ip[3]), nil
}

// Decompose returns a set of Snooflake ID parts.
func Decompose(id uint64) map[string]uint64 {
	const maskSequence = uint64((1<<BitLenSequence - 1) << BitLenMachineID)
	const maskMachineID = uint64(1<<BitLenMachineID - 1)

	msb := id >> 63
	time := id >> (BitLenSequence + BitLenMachineID)
	sequence := id & maskSequence >> BitLenMachineID
	machineID := id & maskMachineID
	return map[string]uint64{
		"id":         id,
		"msb":        msb,
		"time":       time,
		"sequence":   sequence,
		"machine-id": machineID,
	}
}

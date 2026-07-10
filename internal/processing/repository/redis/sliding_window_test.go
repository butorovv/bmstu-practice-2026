package redis

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	goredis "github.com/redis/go-redis/v9"
)

func TestSlidingWindowRepositoryAddPrunesOldValuesAndSetsTTL(t *testing.T) {
	client := newFakeSlidingWindowRedisClient()
	repository := NewSlidingWindowRepository(client, 5*time.Minute)
	baseTime := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	oldEvent := testTelemetryEvent("event-old", baseTime.Add(-61*time.Second), 130)
	currentEvent := testTelemetryEvent("event-current", baseTime, 131)

	if _, err := repository.Add(context.Background(), oldEvent); err != nil {
		t.Fatalf("Add(old) error = %v", err)
	}
	window, err := repository.Add(context.Background(), currentEvent)
	if err != nil {
		t.Fatalf("Add(current) error = %v", err)
	}

	if len(window) != 1 {
		t.Fatalf("window count = %d, want 1", len(window))
	}
	if window[0].EventID != currentEvent.EventID {
		t.Fatalf("window event_id = %q, want %q", window[0].EventID, currentEvent.EventID)
	}

	windowKey := "processing:window:patient-001"
	dataKey := "processing:window:patient-001:events"
	if _, ok := client.zsets[windowKey][currentEvent.EventID]; !ok {
		t.Fatalf("zset does not contain current event_id member")
	}
	if _, ok := client.zsets[windowKey][oldEvent.EventID]; ok {
		t.Fatalf("zset still contains old event_id member")
	}
	if _, ok := client.hashes[dataKey][oldEvent.EventID]; ok {
		t.Fatalf("hash still contains old event payload")
	}
	if client.expirations[windowKey] != 5*time.Minute {
		t.Fatalf("window TTL = %v, want 5m", client.expirations[windowKey])
	}
	if client.expirations[dataKey] != 5*time.Minute {
		t.Fatalf("data TTL = %v, want 5m", client.expirations[dataKey])
	}
}

type fakeSlidingWindowRedisClient struct {
	zsets       map[string]map[string]float64
	hashes      map[string]map[string]string
	expirations map[string]time.Duration
}

func newFakeSlidingWindowRedisClient() *fakeSlidingWindowRedisClient {
	return &fakeSlidingWindowRedisClient{
		zsets:       make(map[string]map[string]float64),
		hashes:      make(map[string]map[string]string),
		expirations: make(map[string]time.Duration),
	}
}

func (f *fakeSlidingWindowRedisClient) ZAdd(
	_ context.Context,
	key string,
	members ...goredis.Z,
) *goredis.IntCmd {
	if _, ok := f.zsets[key]; !ok {
		f.zsets[key] = make(map[string]float64)
	}
	for _, member := range members {
		f.zsets[key][member.Member.(string)] = member.Score
	}

	return goredis.NewIntResult(int64(len(members)), nil)
}

func (f *fakeSlidingWindowRedisClient) ZRangeByScore(
	_ context.Context,
	key string,
	opt *goredis.ZRangeBy,
) *goredis.StringSliceCmd {
	type zmember struct {
		member string
		score  float64
	}

	members := make([]zmember, 0, len(f.zsets[key]))
	for member, score := range f.zsets[key] {
		if scoreInRange(score, opt.Min, opt.Max) {
			members = append(members, zmember{member: member, score: score})
		}
	}
	sort.Slice(members, func(i int, j int) bool {
		if members[i].score == members[j].score {
			return members[i].member < members[j].member
		}

		return members[i].score < members[j].score
	})

	result := make([]string, 0, len(members))
	for _, member := range members {
		result = append(result, member.member)
	}

	return goredis.NewStringSliceResult(result, nil)
}

func (f *fakeSlidingWindowRedisClient) ZRemRangeByScore(
	_ context.Context,
	key string,
	min string,
	max string,
) *goredis.IntCmd {
	var removed int64
	for member, score := range f.zsets[key] {
		if scoreInRange(score, min, max) {
			delete(f.zsets[key], member)
			removed++
		}
	}

	return goredis.NewIntResult(removed, nil)
}

func (f *fakeSlidingWindowRedisClient) HSet(
	_ context.Context,
	key string,
	values ...interface{},
) *goredis.IntCmd {
	if _, ok := f.hashes[key]; !ok {
		f.hashes[key] = make(map[string]string)
	}
	for i := 0; i+1 < len(values); i += 2 {
		field := values[i].(string)
		value := values[i+1].(string)
		f.hashes[key][field] = value
	}

	return goredis.NewIntResult(1, nil)
}

func (f *fakeSlidingWindowRedisClient) HDel(
	_ context.Context,
	key string,
	fields ...string,
) *goredis.IntCmd {
	var removed int64
	for _, field := range fields {
		if _, ok := f.hashes[key][field]; ok {
			delete(f.hashes[key], field)
			removed++
		}
	}

	return goredis.NewIntResult(removed, nil)
}

func (f *fakeSlidingWindowRedisClient) HMGet(
	_ context.Context,
	key string,
	fields ...string,
) *goredis.SliceCmd {
	values := make([]interface{}, 0, len(fields))
	for _, field := range fields {
		value, ok := f.hashes[key][field]
		if !ok {
			values = append(values, nil)
			continue
		}
		values = append(values, value)
	}

	return goredis.NewSliceResult(values, nil)
}

func (f *fakeSlidingWindowRedisClient) Expire(
	_ context.Context,
	key string,
	expiration time.Duration,
) *goredis.BoolCmd {
	f.expirations[key] = expiration
	return goredis.NewBoolResult(true, nil)
}

func scoreInRange(score float64, min string, max string) bool {
	minScore, minExclusive := parseScoreBound(min, true)
	maxScore, maxExclusive := parseScoreBound(max, false)

	if minExclusive {
		if score <= minScore {
			return false
		}
	} else if score < minScore {
		return false
	}

	if maxExclusive {
		if score >= maxScore {
			return false
		}
	} else if score > maxScore {
		return false
	}

	return true
}

func parseScoreBound(value string, lower bool) (float64, bool) {
	if value == "-inf" {
		return -1 << 62, false
	}
	if value == "+inf" {
		return 1 << 62, false
	}

	exclusive := strings.HasPrefix(value, "(")
	value = strings.TrimPrefix(value, "(")
	score, err := strconv.ParseFloat(value, 64)
	if err != nil {
		if lower {
			return -1 << 62, false
		}

		return 1 << 62, false
	}

	return score, exclusive
}

func testTelemetryEvent(eventID string, timestamp time.Time, heartRate int) model.TelemetryEvent {
	return model.TelemetryEvent{
		EventID:   eventID,
		DeviceID:  "device-001",
		PatientID: "patient-001",
		Timestamp: timestamp,
		HeartRate: heartRate,
	}
}

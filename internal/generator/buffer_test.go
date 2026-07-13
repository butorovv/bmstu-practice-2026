package generator

import (
	"testing"
	"time"
)

func TestMeasurementBufferBuildsRecoveryBatchWithoutRemovingData(t *testing.T) {
	buffer := newMeasurementBuffer()
	for index := 0; index < 12; index++ {
		buffer.Push(Measurement{Timestamp: time.Unix(int64(index), 0).UTC(), HeartRate: 80})
	}

	first, closed := buffer.Snapshot(1, 10)
	if closed || len(first) != 10 {
		t.Fatalf("snapshot length = %d, closed=%v", len(first), closed)
	}
	if remaining := buffer.Len(); remaining != 12 {
		t.Fatalf("buffer changed before confirmation: len=%d", remaining)
	}

	buffer.Remove(len(first))
	buffer.Close()
	second, closed := buffer.Snapshot(1, 10)
	if closed || len(second) != 2 {
		t.Fatalf("final snapshot length = %d, closed=%v", len(second), closed)
	}
	buffer.Remove(len(second))
	if _, closed := buffer.Snapshot(1, 10); !closed {
		t.Fatal("drained closed buffer did not report closed")
	}
}

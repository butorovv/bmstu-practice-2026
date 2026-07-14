package generator

import (
	"testing"
	"time"
)

func TestScenarioHeartRateProfiles(t *testing.T) {
	for sequence := uint64(1); sequence <= 20; sequence++ {
		normal := scenarioHeartRate(ModeNormal, 0, sequence)
		if normal < 60 || normal > 100 {
			t.Fatalf("normal heart rate = %d", normal)
		}
		if high := scenarioHeartRate(ModeHighHeartRate, 0, sequence); high <= 120 {
			t.Fatalf("high heart rate = %d", high)
		}
		if nonSelected := scenarioHeartRate(ModeHighHeartRate, 1, sequence); nonSelected > 100 {
			t.Fatalf("non-selected high-mode patient heart rate = %d", nonSelected)
		}
	}

	if spike := scenarioHeartRate(ModeMixed, 1, 1); spike <= 120 {
		t.Fatalf("mixed short spike = %d", spike)
	}
	if afterSpike := scenarioHeartRate(ModeMixed, 1, 2); afterSpike > 100 {
		t.Fatalf("mixed value after short spike = %d", afterSpike)
	}
}

func TestActiveDeviceCountForRampAndSpike(t *testing.T) {
	ramp := testConfig()
	ramp.Mode = ModeRampUp
	ramp.Devices = 100
	ramp.Duration = 100 * time.Second
	if active := activeDeviceCount(ramp, 50*time.Second); active != 50 {
		t.Fatalf("ramp active devices = %d, want 50", active)
	}

	spike := testConfig()
	spike.Mode = ModeSpike
	spike.Devices = 10
	spike.SpikeFactor = 5
	if active := activeDeviceCount(spike, spike.Duration/3); active != 50 {
		t.Fatalf("spike active devices = %d, want 50", active)
	}
	if active := activeDeviceCount(spike, spike.Duration*3/4); active != 10 {
		t.Fatalf("recovery active devices = %d, want 10", active)
	}
}

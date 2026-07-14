package generator

import "time"

func workerCount(cfg Config) int {
	if cfg.Mode == ModeSpike {
		return cfg.Devices * cfg.SpikeFactor
	}
	return cfg.Devices
}

func activeDeviceCount(cfg Config, elapsed time.Duration) int {
	switch cfg.Mode {
	case ModeRampUp:
		progress := float64(elapsed) / float64(cfg.Duration)
		if progress < 0 {
			progress = 0
		}
		if progress > 1 {
			progress = 1
		}
		active := int(float64(cfg.Devices) * progress)
		if active < 1 {
			active = 1
		}
		return active
	case ModeSpike:
		if elapsed >= cfg.Duration/5 && elapsed < cfg.Duration/2 {
			return cfg.Devices * cfg.SpikeFactor
		}
		return cfg.Devices
	default:
		return cfg.Devices
	}
}

func scenarioHeartRate(mode Mode, patientIndex int, sequence uint64) int {
	profile := mode
	switch mode {
	case ModeRampUp, ModeSpike, ModeSoak, ModeChaosReady:
		profile = ModeMixed
	case ModeDuplicate:
		profile = ModeNormal
	}

	normal := 60 + int((sequence*17+uint64(patientIndex)*13)%41)
	high := 125 + int((sequence*11+uint64(patientIndex)*7)%26)

	switch profile {
	case ModeHighHeartRate:
		if patientIndex%5 == 0 {
			return high
		}
	case ModeMixed:
		switch patientIndex % 5 {
		case 0:
			return high
		case 1:
			if sequence%12 == 1 {
				return high
			}
		}
	}
	return normal
}

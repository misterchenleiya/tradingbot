package models

func ResolveSignalSide(signal Signal) (string, bool) {
	if signal.SL == 0 || signal.TP == 0 {
		return "", false
	}
	if signal.Entry == 0 {
		if signal.TP > signal.SL {
			return "long", true
		}
		if signal.TP < signal.SL {
			return "short", true
		}
		return "", false
	}
	if signal.SL < signal.Entry && signal.Entry < signal.TP {
		return "long", true
	}
	if signal.TP < signal.Entry && signal.Entry < signal.SL {
		return "short", true
	}
	return "", false
}

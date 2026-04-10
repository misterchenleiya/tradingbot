package ta

const maxMAPeriod = 180

var defaultMAPeriods = []int{5, 20, 30, 60, 120, 180}

type MAResult struct {
	Periods map[int][]float64
}

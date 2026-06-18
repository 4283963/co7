package aggregator

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"cluster-audit/internal/models"
)

type ScoringWeights struct {
	CPUWeight    float64
	MemoryWeight float64
	LossWeight   float64
	TaskWeight   float64
	BWWeight     float64
}

var defaultWeights = ScoringWeights{
	CPUWeight:    0.30,
	MemoryWeight: 0.20,
	LossWeight:   0.25,
	TaskWeight:   0.15,
	BWWeight:     0.10,
}

const (
	DefaultWindowSize   = 5
	EWMAAlpha           = 0.45
	LowScoreThreshold   = 40.0
	ConsecutiveLowLimit = 3
	MaxAuditLogEntries  = 200
)

type regionRawSample struct {
	CPU       float64
	Memory    float64
	Loss      float64
	Tasks     float64
	BW        float64
	NodeCount int
	Timestamp time.Time
}

type slidingWindow struct {
	samples []regionRawSample
	size    int
}

func newSlidingWindow(size int) *slidingWindow {
	return &slidingWindow{
		samples: make([]regionRawSample, 0, size),
		size:    size,
	}
}

func (w *slidingWindow) push(s regionRawSample) {
	if len(w.samples) >= w.size {
		w.samples = w.samples[1:]
	}
	w.samples = append(w.samples, s)
}

func (w *slidingWindow) len() int {
	return len(w.samples)
}

func (w *slidingWindow) simpleAvg() regionRawSample {
	n := len(w.samples)
	if n == 0 {
		return regionRawSample{}
	}
	var (
		cpu, mem, loss, tasks, bw float64
		nodeSum                   int
	)
	for _, s := range w.samples {
		cpu += s.CPU
		mem += s.Memory
		loss += s.Loss
		tasks += s.Tasks
		bw += s.BW
		nodeSum += s.NodeCount
	}
	return regionRawSample{
		CPU:       cpu / float64(n),
		Memory:    mem / float64(n),
		Loss:      loss / float64(n),
		Tasks:     tasks / float64(n),
		BW:        bw / float64(n),
		NodeCount: nodeSum / n,
	}
}

func (w *slidingWindow) ewma(alpha float64) regionRawSample {
	n := len(w.samples)
	if n == 0 {
		return regionRawSample{}
	}
	acc := w.samples[0]
	for i := 1; i < n; i++ {
		s := w.samples[i]
		acc.CPU = alpha*s.CPU + (1-alpha)*acc.CPU
		acc.Memory = alpha*s.Memory + (1-alpha)*acc.Memory
		acc.Loss = alpha*s.Loss + (1-alpha)*acc.Loss
		acc.Tasks = alpha*s.Tasks + (1-alpha)*acc.Tasks
		acc.BW = alpha*s.BW + (1-alpha)*acc.BW
		acc.NodeCount = int(float64(s.NodeCount)*alpha + float64(acc.NodeCount)*(1-alpha))
	}
	return acc
}

type regionSnapshot struct {
	raw        regionRawSample
	smoothed   regionRawSample
	totalTasks int
	latestTime time.Time
}

type isolationRecord struct {
	isolated   bool
	isolatedAt string
	reason     string
}

type HealthAggregator struct {
	weights     ScoringWeights
	windowSize  int
	mu          sync.RWMutex
	windows     map[string]*slidingWindow
	lastDetails map[string]*models.CenterDetail

	isolationMap    map[string]*isolationRecord
	lowScoreCounter map[string]int
	auditLog        []*models.AuditLogEntry
	onAuditLog      func(entry *models.AuditLogEntry)
}

func NewHealthAggregator() *HealthAggregator {
	return NewHealthAggregatorWithWindow(DefaultWindowSize)
}

func NewHealthAggregatorWithWindow(windowSize int) *HealthAggregator {
	if windowSize < 2 {
		windowSize = 2
	}
	return &HealthAggregator{
		weights:         defaultWeights,
		windowSize:      windowSize,
		windows:         make(map[string]*slidingWindow),
		lastDetails:     make(map[string]*models.CenterDetail),
		isolationMap:    make(map[string]*isolationRecord),
		lowScoreCounter: make(map[string]int),
		auditLog:        make([]*models.AuditLogEntry, 0, MaxAuditLogEntries),
	}
}

func (a *HealthAggregator) OnAuditLog(fn func(entry *models.AuditLogEntry)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.onAuditLog = fn
}

func (a *HealthAggregator) addAuditLog(entry *models.AuditLogEntry) {
	if len(a.auditLog) >= MaxAuditLogEntries {
		a.auditLog = a.auditLog[1:]
	}
	a.auditLog = append(a.auditLog, entry)
	if a.onAuditLog != nil {
		a.onAuditLog(entry)
	}
}

func (a *HealthAggregator) IsolateRegion(region, reason string) (*models.IsolateData, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := models.RegionNameMap[region]; !ok {
		return nil, fmt.Errorf("unknown region: %s", region)
	}

	rec, ok := a.isolationMap[region]
	if !ok {
		rec = &isolationRecord{}
		a.isolationMap[region] = rec
	}

	if rec.isolated {
		return &models.IsolateData{
			Region:      region,
			RegionName:  models.RegionNameMap[region],
			Isolated:    true,
			IsolatedAt:  rec.isolatedAt,
			Reason:      rec.reason,
			HealthScore: a.getLastScore(region),
		}, nil
	}

	now := time.Now().Format(time.RFC3339)
	rec.isolated = true
	rec.isolatedAt = now
	rec.reason = reason

	a.addAuditLog(&models.AuditLogEntry{
		Timestamp:  now,
		Region:     region,
		RegionName: models.RegionNameMap[region],
		EventType:  "isolated",
		Score:      a.getLastScore(region),
		Detail:     fmt.Sprintf("region %s isolated manually, reason: %s", region, reason),
	})

	return &models.IsolateData{
		Region:      region,
		RegionName:  models.RegionNameMap[region],
		Isolated:    true,
		IsolatedAt:  now,
		Reason:      reason,
		HealthScore: a.getLastScore(region),
	}, nil
}

func (a *HealthAggregator) RecoverRegion(region string) (*models.IsolateData, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := models.RegionNameMap[region]; !ok {
		return nil, fmt.Errorf("unknown region: %s", region)
	}

	rec, ok := a.isolationMap[region]
	if !ok || !rec.isolated {
		return &models.IsolateData{
			Region:      region,
			RegionName:  models.RegionNameMap[region],
			Isolated:    false,
			HealthScore: a.getLastScore(region),
		}, nil
	}

	rec.isolated = false
	oldAt := rec.isolatedAt
	rec.isolatedAt = ""
	rec.reason = ""
	a.lowScoreCounter[region] = 0

	a.addAuditLog(&models.AuditLogEntry{
		Timestamp:  time.Now().Format(time.RFC3339),
		Region:     region,
		RegionName: models.RegionNameMap[region],
		EventType:  "recovered",
		Score:      a.getLastScore(region),
		Detail:     fmt.Sprintf("region %s recovered from isolation (was isolated at %s)", region, oldAt),
	})

	return &models.IsolateData{
		Region:      region,
		RegionName:  models.RegionNameMap[region],
		Isolated:    false,
		HealthScore: a.getLastScore(region),
	}, nil
}

func (a *HealthAggregator) GetIsolationStatus() map[string]*isolationRecord {
	result := make(map[string]*isolationRecord)
	for k, v := range a.isolationMap {
		result[k] = v
	}
	return result
}

func (a *HealthAggregator) GetAuditLog(limit int) []*models.AuditLogEntry {
	a.mu.RLock()
	defer a.mu.RUnlock()

	total := len(a.auditLog)
	start := 0
	if limit > 0 && limit < total {
		start = total - limit
	}
	result := make([]*models.AuditLogEntry, total-start)
	copy(result, a.auditLog[start:])
	return result
}

func (a *HealthAggregator) IsRegionIsolated(region string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	rec, ok := a.isolationMap[region]
	return ok && rec.isolated
}

func (a *HealthAggregator) getLastScore(region string) float64 {
	if d, ok := a.lastDetails[region]; ok {
		return d.HealthScore
	}
	return 0
}

func (a *HealthAggregator) Aggregate(metrics []*models.NodeMetrics) *models.ClusterHealth {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(metrics) == 0 {
		return &models.ClusterHealth{
			OverallScore:  0,
			OverallStatus: "unknown",
			GeneratedAt:   time.Now().Format(time.RFC3339),
			Centers:       make([]*models.CenterDetail, 0),
		}
	}

	regionMap := make(map[string][]*models.NodeMetrics)
	for _, m := range metrics {
		regionMap[m.Region] = append(regionMap[m.Region], m)
	}

	regions := []string{models.RegionBeijing, models.RegionShanghai, models.RegionGuangzhou}
	centers := make([]*models.CenterDetail, 0, 3)

	totalNodes := 0
	totalTasks := 0
	totalScore := 0.0
	activeCount := 0
	isolatedCount := 0

	for _, region := range regions {
		nodes, exists := regionMap[region]
		if !exists || len(nodes) == 0 {
			continue
		}

		snap := a.sampleAndSmooth(region, nodes)
		detail := a.calculateCenterDetail(region, nodes, snap)

		isolated := a.isolationMap[region] != nil && a.isolationMap[region].isolated
		detail.Isolated = isolated
		if isolated {
			detail.IsolatedAt = a.isolationMap[region].isolatedAt
			detail.Status = "isolated"
			isolatedCount++
		} else {
			activeCount++
		}

		a.trackLowScore(region, detail.HealthScore)

		detail.LowScoreConsecutive = a.lowScoreCounter[region]

		a.lastDetails[region] = detail
		centers = append(centers, detail)
		totalNodes += detail.NodeCount
		totalTasks += detail.ActiveTasks

		if !isolated {
			totalScore += detail.HealthScore
		}
	}

	overallScore := 0.0
	if activeCount > 0 {
		overallScore = round(totalScore/float64(activeCount), 2)
	}

	sort.Slice(centers, func(i, j int) bool {
		return centers[i].HealthScore > centers[j].HealthScore
	})

	return &models.ClusterHealth{
		OverallScore:  overallScore,
		OverallStatus: statusFromScore(overallScore),
		GeneratedAt:   time.Now().Format(time.RFC3339),
		TotalNodes:    totalNodes,
		TotalTasks:    totalTasks,
		IsolatedCount: isolatedCount,
		ActiveCount:   activeCount,
		Centers:       centers,
	}
}

func (a *HealthAggregator) trackLowScore(region string, score float64) {
	if score < LowScoreThreshold {
		a.lowScoreCounter[region]++
	} else {
		a.lowScoreCounter[region] = 0
	}

	count := a.lowScoreCounter[region]
	if count == ConsecutiveLowLimit {
		a.addAuditLog(&models.AuditLogEntry{
			Timestamp:  time.Now().Format(time.RFC3339),
			Region:     region,
			RegionName: models.RegionNameMap[region],
			EventType:  "low_score_alert",
			Score:      score,
			Detail:     fmt.Sprintf("region %s score %.2f < %.0f for %d consecutive checks, recommend isolation", region, score, LowScoreThreshold, ConsecutiveLowLimit),
		})
	}

	if count > ConsecutiveLowLimit && count%ConsecutiveLowLimit == 0 {
		a.addAuditLog(&models.AuditLogEntry{
			Timestamp:  time.Now().Format(time.RFC3339),
			Region:     region,
			RegionName: models.RegionNameMap[region],
			EventType:  "low_score_persistent",
			Score:      score,
			Detail:     fmt.Sprintf("region %s score %.2f still below threshold for %d consecutive checks", region, score, count),
		})
	}
}

func (a *HealthAggregator) sampleAndSmooth(region string, nodes []*models.NodeMetrics) regionSnapshot {
	nodeCount := len(nodes)
	avgCPU := 0.0
	avgMemory := 0.0
	avgLoss := 0.0
	avgBW := 0.0
	totalTasks := 0
	var latestTime time.Time

	for _, n := range nodes {
		avgCPU += n.CPUUsage
		avgMemory += n.MemoryUsage
		avgLoss += n.PacketLoss
		avgBW += n.NetworkBW
		totalTasks += n.TranscodeTasks
		if n.Timestamp.After(latestTime) {
			latestTime = n.Timestamp
		}
	}
	avgCPU /= float64(nodeCount)
	avgMemory /= float64(nodeCount)
	avgLoss /= float64(nodeCount)
	avgBW /= float64(nodeCount)

	raw := regionRawSample{
		CPU:       avgCPU,
		Memory:    avgMemory,
		Loss:      avgLoss,
		Tasks:     float64(totalTasks),
		BW:        avgBW,
		NodeCount: nodeCount,
		Timestamp: latestTime,
	}

	win, ok := a.windows[region]
	if !ok {
		win = newSlidingWindow(a.windowSize)
		a.windows[region] = win
	}
	win.push(raw)

	simple := win.simpleAvg()
	ewma := win.ewma(EWMAAlpha)

	nf := win.len()
	ewmaW := 0.6 + 0.2*float64(minInt(nf-1, 3))/3.0
	simpleW := 1.0 - ewmaW

	smoothed := regionRawSample{
		CPU:       ewma.CPU*ewmaW + simple.CPU*simpleW,
		Memory:    ewma.Memory*ewmaW + simple.Memory*simpleW,
		Loss:      ewma.Loss*ewmaW + simple.Loss*simpleW,
		Tasks:     ewma.Tasks*ewmaW + simple.Tasks*simpleW,
		BW:        ewma.BW*ewmaW + simple.BW*simpleW,
		NodeCount: int(0.5 + float64(ewma.NodeCount)*ewmaW + float64(simple.NodeCount)*simpleW),
	}

	return regionSnapshot{
		raw:        raw,
		smoothed:   smoothed,
		totalTasks: totalTasks,
		latestTime: latestTime,
	}
}

func (a *HealthAggregator) calculateCenterDetail(region string, nodes []*models.NodeMetrics, snap regionSnapshot) *models.CenterDetail {
	nodeCount := len(nodes)

	hasCritical := false
	hasWarning := false
	for _, n := range nodes {
		if n.Status == "critical" {
			hasCritical = true
		} else if n.Status == "warning" {
			hasWarning = true
		}
	}

	s := snap.smoothed
	smoothedTasks := int(s.Tasks + 0.5)
	if smoothedTasks < 0 {
		smoothedTasks = 0
	}

	score := a.calculateScore(
		s.CPU,
		s.Memory,
		s.Loss,
		s.Tasks,
		s.BW,
		nodeCount,
	)

	baseStatus := statusFromScore(score)
	status := baseStatus
	if hasCritical && score > 50 {
		status = "critical"
	} else if hasWarning && score > 70 {
		status = "warning"
	}

	return &models.CenterDetail{
		Region:         region,
		RegionName:     models.RegionNameMap[region],
		HealthScore:    score,
		Status:         status,
		CPUUsage:       round(snap.raw.CPU, 2),
		MemoryUsage:    round(snap.raw.Memory, 2),
		PacketLoss:     round(snap.raw.Loss, 4),
		NetworkBW:      round(snap.raw.BW, 2),
		ActiveTasks:    snap.totalTasks,
		NodeCount:      nodeCount,
		LastCheckTime:  snap.latestTime.Format(time.RFC3339),
		SmoothedCPU:    round(s.CPU, 2),
		SmoothedMemory: round(s.Memory, 2),
		SmoothedLoss:   round(s.Loss, 4),
		SmoothedTasks:  smoothedTasks,
		SmoothedBW:     round(s.BW, 2),
		WindowSize:     a.windowSize,
		WindowFilled:   a.windows[region].len(),
	}
}

func (a *HealthAggregator) calculateScore(cpu, memory, loss, tasks, bw float64, nodeCount int) float64 {
	cpuScore := scoreCPU(cpu)
	memScore := scoreMemory(memory)
	lossScore := scoreLoss(loss)
	taskScore := scoreTasks(tasks, nodeCount)
	bwScore := scoreBW(bw)

	weighted := cpuScore*a.weights.CPUWeight +
		memScore*a.weights.MemoryWeight +
		lossScore*a.weights.LossWeight +
		taskScore*a.weights.TaskWeight +
		bwScore*a.weights.BWWeight

	if weighted > 100 {
		weighted = 100
	}
	if weighted < 0 {
		weighted = 0
	}

	return round(weighted, 2)
}

func scoreCPU(cpu float64) float64 {
	switch {
	case cpu <= 30:
		return 100
	case cpu <= 50:
		return 100 - (cpu-30)*1.5
	case cpu <= 70:
		return 70 - (cpu-50)*1.5
	case cpu <= 85:
		return 40 - (cpu-70)*2
	case cpu <= 95:
		return 10 - (cpu-85)*1
	default:
		return 0
	}
}

func scoreMemory(mem float64) float64 {
	switch {
	case mem <= 40:
		return 100
	case mem <= 60:
		return 100 - (mem-40)*1.5
	case mem <= 80:
		return 70 - (mem-60)*2
	case mem <= 90:
		return 30 - (mem-80)*2
	default:
		return 10
	}
}

func scoreLoss(loss float64) float64 {
	switch {
	case loss <= 0.1:
		return 100
	case loss <= 0.5:
		return 100 - (loss-0.1)*50
	case loss <= 1.0:
		return 80 - (loss-0.5)*60
	case loss <= 2.0:
		return 50 - (loss-1.0)*30
	case loss <= 3.0:
		return 20 - (loss-2.0)*15
	default:
		return 5
	}
}

func scoreTasks(tasks float64, nodeCount int) float64 {
	if nodeCount == 0 {
		return 50
	}
	avgPerNode := tasks / float64(nodeCount)
	switch {
	case avgPerNode <= 8:
		return 100
	case avgPerNode <= 16:
		return 100 - (avgPerNode-8)*3
	case avgPerNode <= 24:
		return 76 - (avgPerNode-16)*3.5
	case avgPerNode <= 32:
		return 48 - (avgPerNode-24)*4
	case avgPerNode <= 40:
		return 16 - (avgPerNode-32)*2
	default:
		return 0
	}
}

func scoreBW(bw float64) float64 {
	switch {
	case bw >= 800:
		return 100
	case bw >= 600:
		return 80 + (bw-600)*0.1
	case bw >= 400:
		return 60 + (bw-400)*0.1
	case bw >= 200:
		return 40 + (bw-200)*0.1
	case bw >= 100:
		return 20 + (bw-100)*0.2
	default:
		return bw * 0.2
	}
}

func statusFromScore(score float64) string {
	switch {
	case score >= 85:
		return "excellent"
	case score >= 70:
		return "healthy"
	case score >= 50:
		return "warning"
	case score >= 30:
		return "degraded"
	default:
		return "critical"
	}
}

func round(v float64, decimals int) float64 {
	mult := 1.0
	for i := 0; i < decimals; i++ {
		mult *= 10
	}
	val := v * mult
	if val >= 0 {
		return float64(int64(val+0.5)) / mult
	}
	return float64(int64(val-0.5)) / mult
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

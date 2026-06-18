package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"cluster-audit/internal/models"
)

var (
	ErrJobNotFound      = errors.New("job not found")
	ErrJobAlreadyExist  = errors.New("job already exists")
	ErrSchedulerStopped = errors.New("scheduler stopped")
)

type Scheduler interface {
	Start() error
	Stop() error
	RegisterJob(job *Job) error
	UnregisterJob(id JobID) error
	TriggerJob(id JobID) (*JobResult, error)
	GetJob(id JobID) (*Job, error)
	ListJobs() []*Job
	OnJobResult(fn func(result *JobResult))
}

type HealthCheckScheduler struct {
	jobs       map[JobID]*Job
	jobsMu     sync.RWMutex
	running    bool
	runningMu  sync.RWMutex
	stopCh     chan struct{}
	ticker     *time.Ticker
	interval   time.Duration
	wg         sync.WaitGroup
	onResult   func(result *JobResult)
	onResultMu sync.RWMutex

	checker      HealthCheckProvider
	aggregator   HealthAggregatorProvider
	lastHealth   *models.ClusterHealth
	lastHealthMu sync.RWMutex
}

type HealthCheckProvider interface {
	CheckAll(ctx context.Context) ([]*models.NodeMetrics, error)
}

type HealthAggregatorProvider interface {
	Aggregate(metrics []*models.NodeMetrics) *models.ClusterHealth
}

func NewHealthCheckScheduler(
	interval time.Duration,
	checker HealthCheckProvider,
	aggregator HealthAggregatorProvider,
) *HealthCheckScheduler {
	return &HealthCheckScheduler{
		jobs:       make(map[JobID]*Job),
		stopCh:     make(chan struct{}),
		interval:   interval,
		checker:    checker,
		aggregator: aggregator,
	}
}

func (s *HealthCheckScheduler) Start() error {
	s.runningMu.Lock()
	if s.running {
		s.runningMu.Unlock()
		return errors.New("scheduler already running")
	}
	s.running = true
	s.runningMu.Unlock()

	s.registerDefaultJobs()
	s.ticker = time.NewTicker(s.interval)

	s.wg.Add(1)
	go s.runLoop()

	return nil
}

func (s *HealthCheckScheduler) Stop() error {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()

	if !s.running {
		return ErrSchedulerStopped
	}

	s.running = false
	close(s.stopCh)
	s.ticker.Stop()
	s.wg.Wait()

	return nil
}

func (s *HealthCheckScheduler) registerDefaultJobs() {
	healthJob := &Job{
		ID:       JobID("health_check"),
		Name:     "cluster_health_check",
		Interval: s.interval,
		Handler:  s.healthCheckJobHandler,
		Status:   JobStatusPending,
	}
	_ = s.RegisterJob(healthJob)
}

func (s *HealthCheckScheduler) runLoop() {
	defer s.wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-s.stopCh
		cancel()
	}()

	s.runAllJobs(ctx)

	for {
		select {
		case <-s.ticker.C:
			s.runAllJobs(ctx)
		case <-s.stopCh:
			return
		}
	}
}

func (s *HealthCheckScheduler) runAllJobs(ctx context.Context) {
	s.jobsMu.RLock()
	jobs := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	s.jobsMu.RUnlock()

	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(j *Job) {
			defer wg.Done()
			s.executeJob(ctx, j)
		}(job)
	}
	wg.Wait()
}

func (s *HealthCheckScheduler) executeJob(ctx context.Context, job *Job) {
	s.jobsMu.Lock()
	job.Status = JobStatusRunning
	job.LastRunAt = time.Now()
	job.RunCount++
	s.jobsMu.Unlock()

	jobCtx := &JobContext{
		Job:  job,
		Data: make(map[string]interface{}),
	}

	result, err := job.Handler(jobCtx)

	s.jobsMu.Lock()
	if err != nil {
		job.Status = JobStatusFailed
		job.ErrorCount++
		job.LastError = err.Error()
	} else {
		job.Status = JobStatusSuccess
	}
	job.NextRunAt = time.Now().Add(job.Interval)
	s.jobsMu.Unlock()

	if result != nil {
		result.JobID = job.ID
		s.onResultMu.RLock()
		if s.onResult != nil {
			s.onResult(result)
		}
		s.onResultMu.RUnlock()
	}
}

func (s *HealthCheckScheduler) healthCheckJobHandler(jobCtx *JobContext) (*JobResult, error) {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	metrics, err := s.checker.CheckAll(ctx)
	if err != nil {
		return &JobResult{
			Status:    JobStatusFailed,
			StartedAt: start,
			EndedAt:   time.Now(),
			Error:     err,
		}, err
	}

	health := s.aggregator.Aggregate(metrics)

	s.lastHealthMu.Lock()
	s.lastHealth = health
	s.lastHealthMu.Unlock()

	jobCtx.Set("metrics_count", len(metrics))
	jobCtx.Set("overall_score", health.OverallScore)
	jobCtx.Set("center_count", len(health.Centers))

	return &JobResult{
		Status:    JobStatusSuccess,
		StartedAt: start,
		EndedAt:   time.Now(),
		Data:      health,
	}, nil
}

func (s *HealthCheckScheduler) GetLastHealth() *models.ClusterHealth {
	s.lastHealthMu.RLock()
	defer s.lastHealthMu.RUnlock()
	return s.lastHealth
}

func (s *HealthCheckScheduler) RegisterJob(job *Job) error {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()

	if _, exists := s.jobs[job.ID]; exists {
		return ErrJobAlreadyExist
	}
	s.jobs[job.ID] = job
	return nil
}

func (s *HealthCheckScheduler) UnregisterJob(id JobID) error {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()

	if _, exists := s.jobs[id]; !exists {
		return ErrJobNotFound
	}
	delete(s.jobs, id)
	return nil
}

func (s *HealthCheckScheduler) TriggerJob(id JobID) (*JobResult, error) {
	s.jobsMu.RLock()
	job, exists := s.jobs[id]
	s.jobsMu.RUnlock()

	if !exists {
		return nil, ErrJobNotFound
	}

	ctx := context.Background()
	s.executeJob(ctx, job)

	return nil, nil
}

func (s *HealthCheckScheduler) GetJob(id JobID) (*Job, error) {
	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()

	job, exists := s.jobs[id]
	if !exists {
		return nil, ErrJobNotFound
	}
	return job, nil
}

func (s *HealthCheckScheduler) ListJobs() []*Job {
	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()

	jobs := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (s *HealthCheckScheduler) OnJobResult(fn func(result *JobResult)) {
	s.onResultMu.Lock()
	defer s.onResultMu.Unlock()
	s.onResult = fn
}

package scheduler

import "time"

type JobID string

type JobStatus string

const (
	JobStatusPending  JobStatus = "pending"
	JobStatusRunning  JobStatus = "running"
	JobStatusSuccess  JobStatus = "success"
	JobStatusFailed   JobStatus = "failed"
	JobStatusCanceled JobStatus = "canceled"
)

type Job struct {
	ID         JobID
	Name       string
	CronSpec   string
	Interval   time.Duration
	Handler    JobHandler
	LastRunAt  time.Time
	NextRunAt  time.Time
	Status     JobStatus
	RunCount   int64
	ErrorCount int64
	LastError  string
}

type JobResult struct {
	JobID     JobID
	Status    JobStatus
	StartedAt time.Time
	EndedAt   time.Time
	Error     error
	Data      interface{}
}

type JobHandler func(ctx *JobContext) (*JobResult, error)

type JobContext struct {
	Job  *Job
	Data map[string]interface{}
}

func (jc *JobContext) Set(key string, value interface{}) {
	if jc.Data == nil {
		jc.Data = make(map[string]interface{})
	}
	jc.Data[key] = value
}

func (jc *JobContext) Get(key string) (interface{}, bool) {
	if jc.Data == nil {
		return nil, false
	}
	v, ok := jc.Data[key]
	return v, ok
}

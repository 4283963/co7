package models

import "time"

type NodeMetrics struct {
	ID             string
	Region         string
	RegionName     string
	CPUUsage       float64
	TranscodeTasks int
	PacketLoss     float64
	MemoryUsage    float64
	NetworkBW      float64
	Uptime         int64
	Timestamp      time.Time
	Status         string
}

type CenterDetail struct {
	Region              string  `json:"region"`
	RegionName          string  `json:"region_name"`
	HealthScore         float64 `json:"health_score"`
	Status              string  `json:"status"`
	CPUUsage            float64 `json:"cpu_usage"`
	MemoryUsage         float64 `json:"memory_usage"`
	PacketLoss          float64 `json:"packet_loss"`
	NetworkBW           float64 `json:"network_bw_mbps"`
	ActiveTasks         int     `json:"active_transcode_tasks"`
	NodeCount           int     `json:"node_count"`
	LastCheckTime       string  `json:"last_check_time"`
	SmoothedCPU         float64 `json:"smoothed_cpu_usage,omitempty"`
	SmoothedMemory      float64 `json:"smoothed_memory_usage,omitempty"`
	SmoothedLoss        float64 `json:"smoothed_packet_loss,omitempty"`
	SmoothedTasks       int     `json:"smoothed_transcode_tasks,omitempty"`
	SmoothedBW          float64 `json:"smoothed_network_bw_mbps,omitempty"`
	WindowSize          int     `json:"window_size,omitempty"`
	WindowFilled        int     `json:"window_filled,omitempty"`
	Isolated            bool    `json:"isolated"`
	IsolatedAt          string  `json:"isolated_at,omitempty"`
	LowScoreConsecutive int     `json:"low_score_consecutive"`
}

type ClusterHealthResponse struct {
	Code    int            `json:"code"`
	Message string         `json:"message"`
	Data    *ClusterHealth `json:"data"`
}

type ClusterHealth struct {
	OverallScore   float64        `json:"overall_score"`
	OverallStatus  string         `json:"overall_status"`
	GeneratedAt    string         `json:"generated_at"`
	TotalNodes     int            `json:"total_nodes"`
	TotalTasks     int            `json:"total_transcode_tasks"`
	IsolatedCount  int            `json:"isolated_count"`
	ActiveCount    int            `json:"active_count"`
	Centers        []*CenterDetail `json:"centers"`
}

const (
	RegionBeijing = "bj"
	RegionShanghai = "sh"
	RegionGuangzhou = "gz"
)

var RegionNameMap = map[string]string{
	RegionBeijing:   "北京机房",
	RegionShanghai:  "上海机房",
	RegionGuangzhou: "广州机房",
}

type IsolateRequest struct {
	Region string `json:"region" binding:"required"`
	Reason string `json:"reason"`
}

type IsolateResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    *IsolateData `json:"data"`
}

type IsolateData struct {
	Region      string `json:"region"`
	RegionName  string `json:"region_name"`
	Isolated    bool   `json:"isolated"`
	IsolatedAt  string `json:"isolated_at,omitempty"`
	Reason      string `json:"reason,omitempty"`
	HealthScore float64 `json:"health_score"`
}

type AuditLogEntry struct {
	Timestamp string `json:"timestamp"`
	Region    string `json:"region"`
	RegionName string `json:"region_name"`
	EventType string `json:"event_type"`
	Score     float64 `json:"score"`
	Detail    string `json:"detail"`
}

type AuditLogResponse struct {
	Code    int            `json:"code"`
	Message string         `json:"message"`
	Data    []*AuditLogEntry `json:"data"`
}

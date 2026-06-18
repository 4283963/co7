package handler

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"cluster-audit/internal/aggregator"
	"cluster-audit/internal/models"
	"cluster-audit/internal/scheduler"
)

type HealthProvider interface {
	GetLastHealth() *models.ClusterHealth
}

type IsolationManager interface {
	IsolateRegion(region, reason string) (*models.IsolateData, error)
	RecoverRegion(region string) (*models.IsolateData, error)
	IsRegionIsolated(region string) bool
	GetAuditLog(limit int) []*models.AuditLogEntry
}

type HealthHandler struct {
	healthProvider   HealthProvider
	isolationManager IsolationManager
	scheduler        *scheduler.HealthCheckScheduler
}

func NewHealthHandler(hp HealthProvider, im IsolationManager, s *scheduler.HealthCheckScheduler) *HealthHandler {
	return &HealthHandler{
		healthProvider:   hp,
		isolationManager: im,
		scheduler:        s,
	}
}

func (h *HealthHandler) GetClusterHealth(c *gin.Context) {
	health := h.healthProvider.GetLastHealth()
	if health == nil {
		c.JSON(http.StatusOK, &models.ClusterHealthResponse{
			Code:    20001,
			Message: "health data not ready yet, please retry later",
			Data: &models.ClusterHealth{
				OverallScore:  0,
				OverallStatus: "initializing",
				GeneratedAt:   time.Now().Format(time.RFC3339),
				Centers:       make([]*models.CenterDetail, 0),
			},
		})
		return
	}

	c.JSON(http.StatusOK, &models.ClusterHealthResponse{
		Code:    0,
		Message: "success",
		Data:    health,
	})
}

func (h *HealthHandler) GetCenterHealth(c *gin.Context) {
	region := c.Param("region")
	health := h.healthProvider.GetLastHealth()

	if health == nil {
		c.JSON(http.StatusOK, &models.ClusterHealthResponse{
			Code:    20001,
			Message: "health data not ready yet, please retry later",
			Data:    nil,
		})
		return
	}

	for _, center := range health.Centers {
		if center.Region == region {
			singleCluster := &models.ClusterHealth{
				OverallScore:  center.HealthScore,
				OverallStatus: center.Status,
				GeneratedAt:   health.GeneratedAt,
				TotalNodes:    center.NodeCount,
				TotalTasks:    center.ActiveTasks,
				IsolatedCount: boolToInt(center.Isolated),
				ActiveCount:   1 - boolToInt(center.Isolated),
				Centers:       []*models.CenterDetail{center},
			}
			c.JSON(http.StatusOK, &models.ClusterHealthResponse{
				Code:    0,
				Message: "success",
				Data:    singleCluster,
			})
			return
		}
	}

	c.JSON(http.StatusNotFound, &models.ClusterHealthResponse{
		Code:    40401,
		Message: "region not found: " + region,
		Data:    nil,
	})
}

func (h *HealthHandler) IsolateRegion(c *gin.Context) {
	var req models.IsolateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, &models.IsolateResponse{
			Code:    40001,
			Message: "invalid request: " + err.Error(),
			Data:    nil,
		})
		return
	}

	if _, ok := models.RegionNameMap[req.Region]; !ok {
		c.JSON(http.StatusBadRequest, &models.IsolateResponse{
			Code:    40002,
			Message: "unknown region: " + req.Region + ", valid: bj, sh, gz",
			Data:    nil,
		})
		return
	}

	data, err := h.isolationManager.IsolateRegion(req.Region, req.Reason)
	if err != nil {
		c.JSON(http.StatusInternalServerError, &models.IsolateResponse{
			Code:    50001,
			Message: err.Error(),
			Data:    nil,
		})
		return
	}

	c.JSON(http.StatusOK, &models.IsolateResponse{
		Code:    0,
		Message: "region isolated successfully",
		Data:    data,
	})
}

func (h *HealthHandler) RecoverRegion(c *gin.Context) {
	var req models.IsolateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, &models.IsolateResponse{
			Code:    40001,
			Message: "invalid request: " + err.Error(),
			Data:    nil,
		})
		return
	}

	if _, ok := models.RegionNameMap[req.Region]; !ok {
		c.JSON(http.StatusBadRequest, &models.IsolateResponse{
			Code:    40002,
			Message: "unknown region: " + req.Region + ", valid: bj, sh, gz",
			Data:    nil,
		})
		return
	}

	data, err := h.isolationManager.RecoverRegion(req.Region)
	if err != nil {
		c.JSON(http.StatusInternalServerError, &models.IsolateResponse{
			Code:    50001,
			Message: err.Error(),
			Data:    nil,
		})
		return
	}

	c.JSON(http.StatusOK, &models.IsolateResponse{
		Code:    0,
		Message: "region recovered successfully",
		Data:    data,
	})
}

func (h *HealthHandler) GetAuditLog(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	logs := h.isolationManager.GetAuditLog(limit)
	c.JSON(http.StatusOK, &models.AuditLogResponse{
		Code:    0,
		Message: "success",
		Data:    logs,
	})
}

type SchedulerJobResponse struct {
	Code    int       `json:"code"`
	Message string    `json:"message"`
	Data    []JobInfo `json:"data"`
}

type JobInfo struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	RunCount   int64  `json:"run_count"`
	ErrorCount int64  `json:"error_count"`
	LastRunAt  string `json:"last_run_at,omitempty"`
	NextRunAt  string `json:"next_run_at,omitempty"`
	LastError  string `json:"last_error,omitempty"`
}

func (h *HealthHandler) GetSchedulerStatus(c *gin.Context) {
	jobs := h.scheduler.ListJobs()
	jobInfos := make([]JobInfo, 0, len(jobs))

	for _, job := range jobs {
		info := JobInfo{
			ID:         string(job.ID),
			Name:       job.Name,
			Status:     string(job.Status),
			RunCount:   job.RunCount,
			ErrorCount: job.ErrorCount,
			LastError:  job.LastError,
		}
		if !job.LastRunAt.IsZero() {
			info.LastRunAt = job.LastRunAt.Format(time.RFC3339)
		}
		if !job.NextRunAt.IsZero() {
			info.NextRunAt = job.NextRunAt.Format(time.RFC3339)
		}
		jobInfos = append(jobInfos, info)
	}

	c.JSON(http.StatusOK, &SchedulerJobResponse{
		Code:    0,
		Message: "success",
		Data:    jobInfos,
	})
}

func RegisterRoutes(r *gin.Engine, healthHandler *HealthHandler, agg *aggregator.HealthAggregator) {
	apiV1 := r.Group("/api/v1")
	{
		cluster := apiV1.Group("/cluster")
		{
			cluster.GET("/health", healthHandler.GetClusterHealth)
			cluster.GET("/health/:region", healthHandler.GetCenterHealth)
			cluster.POST("/isolate", healthHandler.IsolateRegion)
			cluster.POST("/recover", healthHandler.RecoverRegion)
		}
		system := apiV1.Group("/system")
		{
			system.GET("/scheduler", healthHandler.GetSchedulerStatus)
			system.GET("/audit_log", healthHandler.GetAuditLog)
		}
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

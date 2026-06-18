package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"cluster-audit/internal/aggregator"
	"cluster-audit/internal/checker"
	"cluster-audit/internal/handler"
	"cluster-audit/internal/scheduler"
)

const (
	defaultAddr     = ":8888"
	checkInterval   = 10 * time.Second
	shutdownTimeout = 15 * time.Second
)

func main() {
	gin.SetMode(gin.ReleaseMode)

	healthChecker := checker.NewSimulatedChecker()
	healthAggregator := aggregator.NewHealthAggregator()

	healthScheduler := scheduler.NewHealthCheckScheduler(
		checkInterval,
		healthChecker,
		healthAggregator,
	)

	healthScheduler.OnJobResult(func(result *scheduler.JobResult) {
		if result.Error != nil {
			log.Printf("[Scheduler] Job %s failed after %v: %v",
				result.JobID, result.EndedAt.Sub(result.StartedAt), result.Error)
		} else {
			log.Printf("[Scheduler] Job %s completed in %v, status=%s",
				result.JobID, result.EndedAt.Sub(result.StartedAt), result.Status)
		}
	})

	if err := healthScheduler.Start(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}
	log.Printf("[Scheduler] Started with interval: %v", checkInterval)

	app := gin.New()
	app.Use(gin.Logger())
	app.Use(gin.Recovery())

	app.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		c.Next()
	})

	healthHandler := handler.NewHealthHandler(healthScheduler, healthScheduler)
	handler.RegisterRoutes(app, healthHandler)

	app.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"service": "cluster-audit-api",
			"version": "1.0.0",
			"endpoints": gin.H{
				"cluster_health":   "GET /api/v1/cluster/health",
				"region_health":    "GET /api/v1/cluster/health/:region (bj|sh|gz)",
				"scheduler_status": "GET /api/v1/system/scheduler",
			},
		})
	})

	srv := &http.Server{
		Addr:         defaultAddr,
		Handler:      app,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("[HTTP] Server starting on %s", defaultAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("[Shutdown] Received shutdown signal")

	if err := healthScheduler.Stop(); err != nil {
		log.Printf("[Shutdown] Scheduler stop error: %v", err)
	}
	log.Println("[Shutdown] Scheduler stopped")

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("[Shutdown] Server forced to shutdown: %v", err)
	}

	fmt.Println()
	log.Println("[Shutdown] Server exited gracefully")
}

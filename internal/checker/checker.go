package checker

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"cluster-audit/internal/models"
)

type HealthChecker interface {
	CheckAll(ctx context.Context) ([]*models.NodeMetrics, error)
	CheckRegion(ctx context.Context, region string) ([]*models.NodeMetrics, error)
}

type NodeEndpoint struct {
	ID      string
	Address string
}

type regionEndpoints map[string][]NodeEndpoint

type SimulatedChecker struct {
	endpoints regionEndpoints
	mu        sync.RWMutex
}

func NewSimulatedChecker() *SimulatedChecker {
	return &SimulatedChecker{
		endpoints: regionEndpoints{
			models.RegionBeijing: {
				{ID: "bj-node-01", Address: "http://10.0.1.11:8080/health"},
				{ID: "bj-node-02", Address: "http://10.0.1.12:8080/health"},
				{ID: "bj-node-03", Address: "http://10.0.1.13:8080/health"},
				{ID: "bj-node-04", Address: "http://10.0.1.14:8080/health"},
			},
			models.RegionShanghai: {
				{ID: "sh-node-01", Address: "http://10.0.2.11:8080/health"},
				{ID: "sh-node-02", Address: "http://10.0.2.12:8080/health"},
				{ID: "sh-node-03", Address: "http://10.0.2.13:8080/health"},
			},
			models.RegionGuangzhou: {
				{ID: "gz-node-01", Address: "http://10.0.3.11:8080/health"},
				{ID: "gz-node-02", Address: "http://10.0.3.12:8080/health"},
				{ID: "gz-node-03", Address: "http://10.0.3.13:8080/health"},
				{ID: "gz-node-04", Address: "http://10.0.3.14:8080/health"},
				{ID: "gz-node-05", Address: "http://10.0.3.15:8080/health"},
			},
		},
	}
}

func (c *SimulatedChecker) CheckAll(ctx context.Context) ([]*models.NodeMetrics, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var (
		wg      sync.WaitGroup
		results = make([]*models.NodeMetrics, 0, 16)
		mu      sync.Mutex
	)

	regions := []string{models.RegionBeijing, models.RegionShanghai, models.RegionGuangzhou}

	for _, region := range regions {
		wg.Add(1)
		go func(r string) {
			defer wg.Done()
			metrics, err := c.checkRegionInternal(ctx, r)
			if err != nil {
				return
			}
			mu.Lock()
			results = append(results, metrics...)
			mu.Unlock()
		}(region)
	}

	wg.Wait()
	return results, nil
}

func (c *SimulatedChecker) CheckRegion(ctx context.Context, region string) ([]*models.NodeMetrics, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.checkRegionInternal(ctx, region)
}

func (c *SimulatedChecker) checkRegionInternal(ctx context.Context, region string) ([]*models.NodeMetrics, error) {
	endpoints, ok := c.endpoints[region]
	if !ok {
		return nil, fmt.Errorf("unknown region: %s", region)
	}

	regionName := models.RegionNameMap[region]
	results := make([]*models.NodeMetrics, 0, len(endpoints))

	for _, ep := range endpoints {
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}

		metric := simulateHTTPHealthCheck(ep.ID, region, regionName, ep.Address)
		results = append(results, metric)
	}

	return results, nil
}

func simulateHTTPHealthCheck(nodeID, region, regionName, address string) *models.NodeMetrics {
	_ = address

	cpu := randomFloat(5.0, 95.0)
	mem := randomFloat(10.0, 90.0)
	tasks := int(randomFloat(0, 48))
	loss := randomFloat(0, 5.0)
	bw := randomFloat(50, 1000)
	uptime := int64(randomFloat(3600, 86400*30))

	status := "healthy"
	if cpu > 85 || loss > 3.0 {
		status = "warning"
	}
	if cpu > 92 || loss > 4.5 {
		status = "critical"
	}

	return &models.NodeMetrics{
		ID:             nodeID,
		Region:         region,
		RegionName:     regionName,
		CPUUsage:       round(cpu, 2),
		TranscodeTasks: tasks,
		PacketLoss:     round(loss, 4),
		MemoryUsage:    round(mem, 2),
		NetworkBW:      round(bw, 2),
		Uptime:         uptime,
		Timestamp:      time.Now(),
		Status:         status,
	}
}

func randomFloat(min, max float64) float64 {
	diff := max - min
	n, err := rand.Int(rand.Reader, big.NewInt(100000))
	if err != nil {
		return min
	}
	return min + (float64(n.Int64()) / 100000.0 * diff)
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

// Arquivo: BFT_Sim_parallel/simulator/metrics.go
package simulator

import (
	"fmt"
	"sync"
	"time"
)

// BlockMetric armazena os tempos para um único bloco.
type BlockMetric struct {
	ProposedTime  float64
	FinalizedTime float64
	IsFinalized   bool
}

// MetricsCollector coleta e calcula métricas de desempenho.
type MetricsCollector struct {
	mutex               sync.RWMutex
	blockMetrics        map[int]*BlockMetric
	simulationStartTime time.Time
}

// NewMetricsCollector cria uma nova instância do coletor de métricas.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		blockMetrics:        make(map[int]*BlockMetric),
		simulationStartTime: time.Now(),
	}
}

// RecordProposal registra o tempo em que um bloco foi proposto.
func (mc *MetricsCollector) RecordProposal(blockID int, proposedTime float64) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Só registra se ainda não existir, para o caso de múltiplas propostas em rodadas diferentes
	if _, exists := mc.blockMetrics[blockID]; !exists {
		mc.blockMetrics[blockID] = &BlockMetric{
			ProposedTime: proposedTime,
			IsFinalized:  false,
		}
	}
}

// RecordFinalization registra o tempo em que um bloco foi finalizado.
func (mc *MetricsCollector) RecordFinalization(blockID int, finalizedTime float64) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	if metric, exists := mc.blockMetrics[blockID]; exists {
		if !metric.IsFinalized {
			metric.FinalizedTime = finalizedTime
			metric.IsFinalized = true
		}
	}
}

// PrintResults calcula e imprime as métricas finais.
func (mc *MetricsCollector) PrintResults(simulationTime float64) {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	var totalLatency float64
	finalizedBlocks := 0

	for _, metric := range mc.blockMetrics {
		if metric.IsFinalized {
			latency := metric.FinalizedTime - metric.ProposedTime
			totalLatency += latency
			finalizedBlocks++
		}
	}

	simulationDurationSec := simulationTime / 1000.0 // Converte o tempo de simulação de ms para segundos

	if finalizedBlocks > 0 {
		avgLatency := totalLatency / float64(finalizedBlocks)
		fmt.Printf("\n--- Métricas de Desempenho ---\n")
		fmt.Printf("Total de Blocos Finalizados: %d\n", finalizedBlocks)
		fmt.Printf("Latência Média por Bloco: %.2f ms\n", avgLatency)
	}

	if simulationDurationSec > 0 {
		throughput := float64(finalizedBlocks) / simulationDurationSec
		fmt.Printf("Vazão (Throughput): %.2f blocos/segundo\n", throughput)
	} else {
		fmt.Println("Duração da simulação foi zero, não é possível calcular a vazão.")
	}
	fmt.Printf("--------------------------------\n")
}

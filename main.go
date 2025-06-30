package main

import (
	"BFT-Simulator/simulator"
	"fmt"
	"sync"
)

// --- Constantes e Configuração da Simulação ---

type ProtocolType string

const (
	Tendermint ProtocolType = "tendermint"
)

const (
	ProtocolToRun = Tendermint
	// Altere este valor para ver o impacto no desempenho.
	// Experimente com 4, 8, 16, 32...
	NumNodes         = 32
	NetworkLatencyMs = 100
	SimulationTimeMs = 240000 // 15 segundos
	Debug            = false
)

// --- Função Principal ---

func main() {
	fmt.Printf("--- Configurando Simulação para %d Nós ---\n", NumNodes)

	sim := simulator.NewSimulation()
	metrics := simulator.NewMetricsCollector()
	network := simulator.NewGossipNetwork(sim, NetworkLatencyMs)

	// A fábrica agora passa NumNodes para que o protocolo possa se adaptar.
	factory := func(node *simulator.Node, sim *simulator.Simulation, mc *simulator.MetricsCollector) simulator.ConsensusProtocol {
		// Passa NumNodes para o construtor do protocolo.
		return NewTendermintProtocol(node, sim, mc, NumNodes)
	}

	nodes := make([]*simulator.Node, NumNodes)
	for i := 0; i < NumNodes; i++ {
		nodes[i] = simulator.NewNode(i, network, sim, metrics, factory)
	}

	var wg sync.WaitGroup
	wg.Add(NumNodes)

	for _, node := range nodes {
		go node.Run(&wg)
	}

	fmt.Println("Aguardando todos os nós iniciarem...")
	wg.Wait()
	fmt.Println("Todos os nós estão prontos. Iniciando a simulação.")

	fmt.Printf("\n--- Rodando Simulação por %.1fms ---\n", float64(SimulationTimeMs))
	sim.RunUntil(float64(SimulationTimeMs))

	fmt.Println("\n--- Simulação Finalizada ---")
	metrics.PrintResults(float64(SimulationTimeMs))
}

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
	ProtocolToRun    = Tendermint
	NumNodes         = 32
	NetworkLatencyMs = 50
	SimulationTimeMs = 600000 // Aumentado para 5 segundos para ver mais atividade
	Debug            = true
)

// --- Função Principal ---

func main() {
	fmt.Println("--- Configurando a Simulação ---")

	sim := simulator.NewSimulation()
	network := simulator.NewGossipNetwork(sim, NetworkLatencyMs)
	factory := func(node *simulator.Node, sim *simulator.Simulation) simulator.ConsensusProtocol {
		return NewTendermintProtocol(node, sim)
	}

	nodes := make([]*simulator.Node, NumNodes)
	for i := 0; i < NumNodes; i++ {
		nodes[i] = simulator.NewNode(i, network, sim, factory)
	}

	// *** SOLUÇÃO DE SINCRONIZAÇÃO ***
	// Usamos um WaitGroup para garantir que todos os nós agendem seu primeiro
	// evento ANTES que o loop da simulação comece.
	var wg sync.WaitGroup
	wg.Add(NumNodes) // Esperaremos por 'NumNodes' sinais de "pronto".

	// Inicia a goroutine de cada nó. Elas ficarão esperando por mensagens.
	for _, node := range nodes {
		// Passamos a responsabilidade de iniciar o protocolo para a goroutine do nó,
		// que então sinalizará quando estiver pronta.
		go node.Run(&wg)
	}

	// A goroutine 'main' irá pausar aqui e esperar até que todas as outras
	// goroutines tenham chamado 'wg.Done()'.
	fmt.Println("Aguardando todos os nós iniciarem...")
	wg.Wait()
	fmt.Println("Todos os nós estão prontos. Iniciando a simulação.")

	// 5. Inicia a simulação.
	// Agora temos 100% de certeza de que a fila de eventos não está vazia.
	fmt.Printf("\n--- Rodando Simulação por %.1fms ---\n", float64(SimulationTimeMs))
	sim.RunUntil(float64(SimulationTimeMs))

	fmt.Println("\n--- Simulação Finalizada ---")
}

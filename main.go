package main

import (
	"BFT-Simulator/simulator"
	"fmt"
	_ "time"

	"github.com/fschuetz04/simgo"
)

// --- Constantes e Configuração da Simulação ---

type ProtocolType string

const (
	Tendermint ProtocolType = "tendermint"
	HotStuff   ProtocolType = "hotstuff"
)

const (
	// Selecione o protocolo para executar aqui
	ProtocolToRun = Tendermint

	NumNodes         = 16
	NetworkLatencyMs = 50
	ViewTimeoutMs    = 4000 // Timeout para uma visão/rodada inteira
	Debug            = false
)

// --- Função Principal ---

func main() {
	sim := simgo.NewSimulation()

	// O processo da rede é necessário para que ela possa agendar seus próprios eventos de latência.
	sim.Process(func(proc simgo.Process) {
		var network simulator.Network
		var factory simulator.ProtocolFactory

		switch ProtocolToRun {
		case Tendermint:
			fmt.Println("--- Iniciando Simulação com Tendermint ---")
			network = simulator.NewGossipNetwork(proc, NetworkLatencyMs)
			factory = func(node *simulator.Node) simulator.ConsensusProtocol {
				return NewTendermintProtocol(node)
			}
		//case HotStuff:
		//	fmt.Println("--- Iniciando Simulação com HotStuff ---")
		//	network = simulator.NewStarNetwork(proc, NetworkLatencyMs)
		//	factory = func(node *simulator.Node) simulator.ConsensusProtocol {
		//		return NewHotStuffProtocol(node)
		//	}
		default:
			fmt.Println("Protocolo desconhecido.")
			return
		}

		// Inicia os processos dos nós genéricos, injetando a fábrica do protocolo selecionado.
		for i := 0; i < NumNodes; i++ {
			sim.ProcessReflect(simulator.RunNodeProcess, i, network, factory)
		}
	})

	sim.RunUntil(60000000) // Executa por 20 segundos de tempo simulado.
	fmt.Println("\n--- Simulação Finalizada ---")
}

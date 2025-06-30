package simulator

import (
	"fmt"
	"github.com/fschuetz04/simgo"
)

// Node representa um participante genérico na simulação.
// Ele encapsula o processo simgo e o protocolo de consenso que está executando.
type Node struct {
	ID       int
	Proc     simgo.Process
	Network  Network
	Protocol ConsensusProtocol
}

// ProtocolFactory é uma função que cria uma instância de um protocolo de consenso específico.
type ProtocolFactory func(node *Node) ConsensusProtocol

// RunNodeProcess é a função principal que executa a lógica de um nó genérico.
// Ela escuta por mensagens e as repassa para o protocolo de consenso manipulá-las.
func RunNodeProcess(proc simgo.Process, id int, network Network, factory ProtocolFactory) {
	node := &Node{
		ID:      id,
		Proc:    proc,
		Network: network,
	}
	// A fábrica cria a implementação específica do protocolo (ex: Tendermint).
	node.Protocol = factory(node)

	messageChannel := make(chan Message, 4096)
	network.Register(id, messageChannel)

	// Inicia a máquina de estados interna do protocolo em uma goroutine separada.
	go node.Protocol.Start()

	for {
		// Aguarda por novas mensagens no canal.
		select {
		case msg := <-messageChannel:
			node.Protocol.HandleMessage(msg)
		default:
			if proc.Aborted() {
				fmt.Printf("Nó %d encerrando.\n", id)
				return
			}
		}
	}
}

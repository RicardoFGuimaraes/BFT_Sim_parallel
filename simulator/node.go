package simulator

import (
	"fmt"
	"sync"
)

// Node representa um participante na simulação. (struct sem alterações)
type Node struct {
	ID             int
	Network        Network
	Protocol       ConsensusProtocol
	messageChannel chan Message
}

// ProtocolFactory continua o mesmo
type ProtocolFactory func(node *Node, sim *Simulation) ConsensusProtocol

// NewNode continua o mesmo
func NewNode(id int, network Network, sim *Simulation, factory ProtocolFactory) *Node {
	node := &Node{
		ID:             id,
		Network:        network,
		messageChannel: make(chan Message, 4096),
	}
	node.Protocol = factory(node, sim)
	network.Register(id, node.messageChannel)
	return node
}

// Run foi atualizado para aceitar o WaitGroup
func (n *Node) Run(wg *sync.WaitGroup) {
	// Inicia a máquina de estados do protocolo.
	// O método Start() agora é responsável por agendar o primeiro evento.
	n.Protocol.Start()

	// *** SINALIZAÇÃO ***
	// Depois de agendar seu evento inicial, o nó avisa ao WaitGroup que ele "nasceu".
	wg.Done()

	// Loop para processar mensagens recebidas, como antes.
	for msg := range n.messageChannel {
		n.Protocol.HandleMessage(msg)
	}

	fmt.Printf("Nó %d encerrando.\n", n.ID)
}

// CloseChannel continua o mesmo
func (n *Node) CloseChannel() {
	close(n.messageChannel)
}

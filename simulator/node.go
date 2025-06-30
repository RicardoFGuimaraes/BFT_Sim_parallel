package simulator

import (
	"fmt"
	"sync"
)

// Node representa um participante na simulação.
type Node struct {
	ID             int
	Network        Network
	Protocol       ConsensusProtocol
	messageChannel chan Message
}

// ProtocolFactory foi atualizada para que o protocolo conheça o tamanho da rede.
type ProtocolFactory func(node *Node, sim *Simulation, mc *MetricsCollector) ConsensusProtocol

// NewNode foi atualizado para passar os novos argumentos para a fábrica.
func NewNode(id int, network Network, sim *Simulation, mc *MetricsCollector, factory ProtocolFactory) *Node {
	node := &Node{
		ID:             id,
		Network:        network,
		messageChannel: make(chan Message, 4096),
	}

	// O número de nós e outras dependências agora são passados para a fábrica.
	node.Protocol = factory(node, sim, mc)

	network.Register(id, node.messageChannel)
	return node
}

// Run foi atualizado para aceitar o WaitGroup
func (n *Node) Run(wg *sync.WaitGroup) {
	// Inicia a máquina de estados do protocolo, que agenda o primeiro evento.
	n.Protocol.Start()

	// Avisa ao WaitGroup que este nó terminou sua inicialização.
	wg.Done()

	// Loop para processar mensagens recebidas.
	for msg := range n.messageChannel {
		n.Protocol.HandleMessage(msg)
	}
	fmt.Printf("Nó %d encerrando.\n", n.ID)
}

// CloseChannel não precisa de alterações
func (n *Node) CloseChannel() {
	close(n.messageChannel)
}

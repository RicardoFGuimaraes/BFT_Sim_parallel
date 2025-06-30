package simulator

import "time"

// Network define a interface para a camada de comunicação.
// Permite registrar nós e enviar mensagens unicast ou broadcast.
type Network interface {
	Register(nodeID int, receiver chan<- Message)
	Broadcast(senderID int, payload interface{})
	Unicast(senderID, receiverID int, payload interface{})
}

// ConsensusProtocol define o contrato que cada algoritmo de consenso deve implementar.
// O simulador irá interagir com os protocolos através desta interface.
type ConsensusProtocol interface {
	Start()
	HandleMessage(msg Message)
}
type Event interface {
	Timestamp() time.Time
	Handle(sim interface{})
}

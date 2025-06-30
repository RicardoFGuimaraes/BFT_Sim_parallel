package simulator

import (
	"sync"
)

type GossipNetwork struct {
	sim        *Simulation // Referência ao motor de simulação.
	nodeChans  map[int]chan<- Message
	latency    float64
	nodesMutex sync.RWMutex
}

func NewGossipNetwork(sim *Simulation, latencyMs int) *GossipNetwork {
	return &GossipNetwork{
		sim:       sim,
		nodeChans: make(map[int]chan<- Message),
		latency:   float64(latencyMs),
	}
}

// Register adiciona o canal de um nó à rede para recebimento de mensagens.
func (net *GossipNetwork) Register(nodeID int, channel chan<- Message) {
	net.nodesMutex.Lock()
	defer net.nodesMutex.Unlock()
	net.nodeChans[nodeID] = channel
}

// sendMessage agora agenda um evento em vez de criar um processo.
func (net *GossipNetwork) sendMessage(channel chan<- Message, msg Message) {
	// Agenda a entrega da mensagem para o futuro.
	net.sim.Schedule(func() {
		// Envio não-bloqueante para evitar deadlocks se o canal estiver cheio.
		select {
		case channel <- msg:
		default:
			// Pacote descartado (canal cheio ou nó inativo).
		}
	}, net.latency)
}

// Broadcast e Unicast usarão o sendMessage refatorado.
func (net *GossipNetwork) Broadcast(senderID int, payload interface{}) {
	net.nodesMutex.RLock()
	defer net.nodesMutex.RUnlock()

	msg := Message{SenderID: senderID, ReceiverID: -1, Payload: payload}
	for id, ch := range net.nodeChans {
		if id != senderID {
			net.sendMessage(ch, msg)
		}
	}
}

// Unicast envia uma mensagem para um único nó.
func (net *GossipNetwork) Unicast(senderID, receiverID int, payload interface{}) {
	//net.nodesMutex.RLock()
	//channel, ok := net.nodeChans[receiverID]
	//net.nodesMutex.RUnlock()
	//
	//if !ok {
	//	return // Nó de destino não registrado.
	//}
	//
	//msg := Message{
	//	SenderID:   senderID,
	//	ReceiverID: receiverID,
	//	Payload:    payload,
	//}
	//
	//// Inicia um novo processo simgo para o envio da mensagem.
	//net.proc.Process(func(proc simgo.Process) {
	//	net.sendMessage(proc, channel, msg)
	//})
}

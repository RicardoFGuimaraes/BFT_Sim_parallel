package simulator

import (
	"sync"

	"github.com/fschuetz04/simgo"
)

// StarNetwork implementa a interface Network com um modelo de comunicação
// centralizado no líder, típico do HotStuff.
type StarNetwork struct {
	proc       simgo.Process
	nodeChans  map[int]chan<- Message
	latency    float64
	nodesMutex sync.RWMutex
}

// NewStarNetwork cria uma nova instância da rede em estrela.
func NewStarNetwork(proc simgo.Process, latencyMs int) *StarNetwork {
	return &StarNetwork{
		proc:      proc,
		nodeChans: make(map[int]chan<- Message),
		latency:   float64(latencyMs),
	}
}

func (net *StarNetwork) Register(nodeID int, channel chan<- Message) {
	net.nodesMutex.Lock()
	defer net.nodesMutex.Unlock()
	net.nodeChans[nodeID] = channel
}

// Broadcast só pode ser chamado pelo líder atual. Envia para todas as réplicas.
func (net *StarNetwork) Broadcast(senderID int, payload interface{}) {
	currentLeader := getLeader(payload) // Função auxiliar para determinar o líder da visão
	if senderID != currentLeader {
		// Em uma simulação real, isso seria um erro de protocolo.
		// Apenas o líder pode fazer broadcast.
		return
	}

	net.nodesMutex.RLock()
	defer net.nodesMutex.RUnlock()

	msg := Message{SenderID: senderID, ReceiverID: -1, Payload: payload}
	for id := range net.nodeChans {
		if id == senderID {
			continue
		}
		net.proc.ProcessReflect(net.sendMessage, id, msg)
	}
}

// Unicast só pode ser chamado por uma réplica para enviar ao líder atual.
func (net *StarNetwork) Unicast(senderID, receiverID int, payload interface{}) {
	currentLeader := getLeader(payload)
	if receiverID != currentLeader || senderID == currentLeader {
		// Apenas réplicas podem enviar para o líder.
		return
	}

	net.nodesMutex.RLock()
	defer net.nodesMutex.RUnlock()

	if _, ok := net.nodeChans[receiverID]; !ok {
		return
	}

	msg := Message{SenderID: senderID, ReceiverID: receiverID, Payload: payload}
	net.proc.ProcessReflect(net.sendMessage, receiverID, msg)
}

func (net *StarNetwork) sendMessage(proc simgo.Process, receiverID int, msg Message) {
	proc.Wait(proc.Timeout(net.latency))
	net.nodesMutex.RLock()
	channel, ok := net.nodeChans[receiverID]
	net.nodesMutex.RUnlock()
	if ok {
		channel <- msg
	}
}

// getLeader é uma função auxiliar para extrair a visão (view) do payload
// e determinar quem é o líder para essa visão.
func getLeader(payload interface{}) int {
	view := 0 // Visão padrão
	switch p := payload.(type) {
	case HSProposal:
		view = p.View
	case HSVote:
		view = p.View
	case QuorumCertificate:
		view = p.View
	}
	// Em uma simulação real, NumNodes seria um parâmetro de configuração.
	return view % 4 // Usando o mesmo NumNodes da main.go
}

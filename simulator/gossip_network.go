package simulator

import (
	"github.com/fschuetz04/simgo"
	"sync"
)

// GossipNetwork implementa a interface Network com um modelo de comunicação gossip.
type GossipNetwork struct {
	proc       simgo.Process
	nodeChans  map[int]chan<- Message
	latency    float64
	nodesMutex sync.RWMutex
}

// NewGossipNetwork cria uma nova instância da rede gossip.
func NewGossipNetwork(proc simgo.Process, latencyMs int) *GossipNetwork {
	return &GossipNetwork{
		proc:      proc,
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

// sendMessage é um processo simgo que envia uma mensagem para um canal após um atraso.
// Esta função auxiliar é usada tanto por Broadcast quanto por Unicast.
func (net *GossipNetwork) sendMessage(proc simgo.Process, channel chan<- Message, msg Message) {
	proc.Wait(proc.Timeout(net.latency))

	// Se o processo que está enviando a mensagem foi abortado durante a latência,
	// não devemos tentar enviar.
	if proc.Aborted() {
		return
	}

	// Usamos um select com um default para fazer um envio não-bloqueante.
	// Se o canal do receptor estiver cheio ou fechado, o default será executado
	// e a mensagem será descartada. Isso evita deadlocks ou panics.
	select {
	case channel <- msg:
		// A mensagem foi enviada com sucesso.
	default:
		// O canal do receptor não está pronto. Em uma simulação de rede,
		// isso pode ser interpretado como um pacote perdido devido a congestionamento
		// ou porque o nó receptor terminou. Não fazemos nada.
	}
}

// Broadcast envia uma mensagem para todos os outros nós com um atraso de rede.
func (net *GossipNetwork) Broadcast(senderID int, payload interface{}) {
	net.nodesMutex.RLock()

	msg := Message{
		SenderID:   senderID,
		ReceiverID: -1, // -1 indica broadcast
		Payload:    payload,
	}

	// Copia os canais para um slice para evitar manter o lock durante a iteração.
	var channelsToBroadcast []chan<- Message
	for id, ch := range net.nodeChans {
		if id != senderID {
			channelsToBroadcast = append(channelsToBroadcast, ch)
		}
	}
	net.nodesMutex.RUnlock()

	for _, channel := range channelsToBroadcast {
		// Inicia um novo processo simgo para cada envio de mensagem.
		net.proc.Process(func(proc simgo.Process) {
			net.sendMessage(proc, channel, msg)
		})
	}
}

// Unicast envia uma mensagem para um único nó.
func (net *GossipNetwork) Unicast(senderID, receiverID int, payload interface{}) {
	net.nodesMutex.RLock()
	channel, ok := net.nodeChans[receiverID]
	net.nodesMutex.RUnlock()

	if !ok {
		return // Nó de destino não registrado.
	}

	msg := Message{
		SenderID:   senderID,
		ReceiverID: receiverID,
		Payload:    payload,
	}

	// Inicia um novo processo simgo para o envio da mensagem.
	net.proc.Process(func(proc simgo.Process) {
		net.sendMessage(proc, channel, msg)
	})
}

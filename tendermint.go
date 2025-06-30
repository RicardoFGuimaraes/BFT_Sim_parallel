package main

import (
	"BFT-Simulator/simulator"
	"fmt"
	"sync"
)

// --- Tipos Específicos do Protocolo Tendermint (sem alterações) ---
type TendermintStep int

const (
	Propose TendermintStep = iota
	Prevote
	Precommit
)

func (s TendermintStep) String() string {
	return [...]string{"Propose", "Prevote", "Precommit"}[s]
}

type Block struct{ ID int }
type Vote struct {
	BlockID *int
	VoterID int
}
type Proposal struct {
	Block  Block
	Height int
	Round  int
}
type PrevotePayload struct {
	Vote   Vote
	Height int
	Round  int
}
type PrecommitPayload struct {
	Vote   Vote
	Height int
	Round  int
}
type DecisionPayload struct {
	Height int
}

// --- Constantes de Timeout (sem alterações) ---
const (
	TimeoutProposeMs   = 500
	TimeoutPrevoteMs   = 500
	TimeoutPrecommitMs = 500
	TimeoutDeltaMs     = 100
)

// --- Implementação do Protocolo Tendermint (Corrigida) ---
type TendermintProtocol struct {
	node       *simulator.Node
	sim        *simulator.Simulation // Referência ao motor de simulação
	quorumSize int

	stateMutex sync.Mutex   // Protege o acesso a todos os campos de estado abaixo
	votesMutex sync.RWMutex // Protege os mapas de votos

	// Estado do Consenso
	height     int
	round      int
	step       TendermintStep
	proposal   *Block
	lockedVote *Vote

	// Mapas de votos
	prevotes   map[int]*Vote
	precommits map[int]*Vote
}

// NewTendermintProtocol cria uma nova instância do protocolo para o motor orientado a eventos.
func NewTendermintProtocol(node *simulator.Node, sim *simulator.Simulation) simulator.ConsensusProtocol {
	f := (NumNodes - 1) / 3
	return &TendermintProtocol{
		node:       node,
		sim:        sim,
		quorumSize: 2*f + 1,
		height:     1,
		round:      0,
		step:       Propose,
		prevotes:   make(map[int]*Vote),
		precommits: make(map[int]*Vote),
	}
}

// Start inicia o processo de consenso para o nó, agendando o início da primeira altura.
func (tp *TendermintProtocol) Start() {
	// Agenda o início da primeira altura/rodada com um atraso de 0.
	tp.sim.Schedule(tp.enterNewHeight, 0)
}

// enterNewHeight é o ponto de entrada para uma nova altura de consenso.
func (tp *TendermintProtocol) enterNewHeight() {
	tp.stateMutex.Lock()
	tp.log("Iniciando nova altura.")
	tp.step = Propose
	tp.round = 0              // Sempre começa na rodada 0 para uma nova altura
	tp.resetRoundState(false) // Limpa o estado da rodada anterior
	tp.stateMutex.Unlock()

	tp.startRound()
}

// startRound contém a lógica para iniciar uma nova rodada (Propose -> Prevote -> Precommit).
func (tp *TendermintProtocol) startRound() {
	tp.stateMutex.Lock()
	currentHeight := tp.height
	currentRound := tp.round
	tp.log("Iniciando fase de Propose.")
	tp.stateMutex.Unlock()

	tp.doProposePhase()

	// Agenda a transição para a fase de Prevote após o timeout de Propose.
	timeoutPropose := tp.getTimeout(currentRound)
	tp.sim.Schedule(func() { tp.onTimeoutPropose(currentHeight, currentRound) }, float64(TimeoutProposeMs)+timeoutPropose)
}

// doProposePhase executa a lógica da fase de proposta.
func (tp *TendermintProtocol) doProposePhase() {
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()

	proposerID := tp.getProposer(tp.height, tp.round)
	if tp.node.ID == proposerID {
		var block Block
		if tp.lockedVote != nil && tp.lockedVote.BlockID != nil {
			block = Block{ID: *tp.lockedVote.BlockID}
		} else {
			// Cria um ID de bloco único para fins de simulação.
			block = Block{ID: tp.height*10000 + tp.round}
		}

		proposal := Proposal{Block: block, Height: tp.height, Round: tp.round}
		tp.proposal = &block
		tp.log(fmt.Sprintf("É o proponente, enviando proposta para bloco %d", block.ID))
		tp.node.Network.Broadcast(tp.node.ID, proposal)
	}
}

// onTimeoutPropose é chamado quando o timer da fase de Propose expira.
func (tp *TendermintProtocol) onTimeoutPropose(expectedHeight, expectedRound int) {
	tp.stateMutex.Lock()
	// Se o estado mudou (ex: já avançamos para outra rodada/altura), não faz nada.
	if tp.height != expectedHeight || tp.round != expectedRound {
		tp.stateMutex.Unlock()
		return
	}

	tp.log("Timeout de Propose, iniciando fase de Prevote.")
	tp.step = Prevote
	tp.broadcastVote(true) // Envia o voto de Prevote
	currentRound := tp.round
	tp.stateMutex.Unlock()

	// Agenda a transição para a fase de Precommit.
	timeoutPrevote := tp.getTimeout(currentRound)
	tp.sim.Schedule(func() { tp.onTimeoutPrevote(expectedHeight, expectedRound) }, float64(TimeoutPrevoteMs)+timeoutPrevote)
}

// onTimeoutPrevote é chamado quando o timer da fase de Prevote expira.
func (tp *TendermintProtocol) onTimeoutPrevote(expectedHeight, expectedRound int) {
	tp.stateMutex.Lock()
	if tp.height != expectedHeight || tp.round != expectedRound {
		tp.stateMutex.Unlock()
		return
	}

	// Verifica se um Quorum (Polka) foi alcançado na fase de Prevote.
	tp.votesMutex.RLock()
	polkaBlockID, hasPolka := tp.getQuorum(tp.prevotes)
	tp.votesMutex.RUnlock()

	tp.log("Timeout de Prevote, iniciando fase de Precommit.")
	tp.step = Precommit

	if hasPolka {
		logMsg := "formou Polka para <nil>"
		if polkaBlockID != nil {
			logMsg = fmt.Sprintf("formou Polka para bloco %d", *polkaBlockID)
		}
		tp.log(logMsg)
		tp.lockedVote = &Vote{BlockID: polkaBlockID}
	} else {
		tp.log("Não formou Polka, votando <nil>.")
		tp.lockedVote = &Vote{BlockID: nil}
	}

	tp.broadcastVote(false) // Envia o voto de Precommit
	currentRound := tp.round
	tp.stateMutex.Unlock()

	// Agenda a decisão final da rodada.
	timeoutPrecommit := tp.getTimeout(currentRound)
	tp.sim.Schedule(func() { tp.onTimeoutPrecommit(expectedHeight, expectedRound) }, float64(TimeoutPrecommitMs)+timeoutPrecommit)
}

// onTimeoutPrecommit é chamado ao final da rodada para decidir o que fazer a seguir.
func (tp *TendermintProtocol) onTimeoutPrecommit(expectedHeight, expectedRound int) {
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()

	if tp.height != expectedHeight || tp.round != expectedRound {
		return // Já avançou, não faz nada.
	}

	// Verifica se há um Quorum de Precommits.
	tp.votesMutex.RLock()
	commitBlockID, hasCommitQuorum := tp.getQuorum(tp.precommits)
	tp.votesMutex.RUnlock()

	// DECISÃO: Se um quorum para um bloco específico foi alcançado, finalize-o.
	if hasCommitQuorum && commitBlockID != nil {
		tp.log(fmt.Sprintf("BLOCO %d FINALIZADO! Notificando outros nós.", *commitBlockID))

		// Transmite a decisão para que outros nós possam sincronizar.
		tp.node.Network.Broadcast(tp.node.ID, DecisionPayload{Height: tp.height})

		// Avança para a próxima altura.
		tp.height++
		// A rodada será 0 na próxima chamada a enterNewHeight

		// Agenda o início da próxima altura.
		tp.sim.Schedule(tp.enterNewHeight, 1.0) // Pequeno delay para permitir a propagação da mensagem de decisão.
	} else {
		// Se não houve decisão (timeout ou quorum de <nil>), avança para a próxima rodada.
		tp.log("Timeout ou <nil> quorum, avançando para a próxima rodada.")
		tp.round++
		tp.resetRoundState(true)

		// Agenda o início da próxima rodada.
		tp.sim.Schedule(tp.startRound, 1.0)
	}
}

// HandleMessage processa as mensagens recebidas da rede.
func (tp *TendermintProtocol) HandleMessage(msg simulator.Message) {
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()

	switch payload := msg.Payload.(type) {
	case Proposal:
		// Aceita a proposta apenas se estiver na altura/rodada correta e na fase de Propose.
		if payload.Height == tp.height && payload.Round == tp.round && tp.step == Propose && tp.proposal == nil {
			tp.proposal = &payload.Block
			tp.log(fmt.Sprintf("Recebeu proposta de %d para o bloco %d", msg.SenderID, payload.Block.ID))
		}

	case PrevotePayload:
		if payload.Height == tp.height && payload.Round == tp.round {
			tp.votesMutex.Lock()
			if _, exists := tp.prevotes[payload.Vote.VoterID]; !exists {
				tp.prevotes[payload.Vote.VoterID] = &payload.Vote
			}
			tp.votesMutex.Unlock()
		}

	case PrecommitPayload:
		if payload.Height == tp.height && payload.Round == tp.round {
			tp.votesMutex.Lock()
			if _, exists := tp.precommits[payload.Vote.VoterID]; !exists {
				tp.precommits[payload.Vote.VoterID] = &payload.Vote
			}
			tp.votesMutex.Unlock()
		}

	case DecisionPayload:
		// Se receber uma notificação de decisão para uma altura que já alcançamos ou superamos, ignora.
		// Se for para uma altura futura, sincroniza e avança.
		if payload.Height >= tp.height {
			tp.log(fmt.Sprintf("Recebeu notificação de decisão para altura %d. Sincronizando.", payload.Height))
			tp.height = payload.Height + 1

			// Agenda o início da nova altura, efetivamente cancelando timers pendentes para a altura antiga.
			tp.sim.Schedule(tp.enterNewHeight, 1.0)
		}
	}
}

// --- Funções Auxiliares (com correções) ---

// broadcastVote envia o voto atual do nó (Prevote ou Precommit).
func (tp *TendermintProtocol) broadcastVote(isPrevote bool) {
	// Esta função é chamada com stateMutex JÁ ADQUIRIDO.
	var votePayload interface{}
	vote := &Vote{VoterID: tp.node.ID}

	if isPrevote {
		if tp.proposal != nil {
			vote.BlockID = &tp.proposal.ID
		}
		votePayload = PrevotePayload{Vote: *vote, Height: tp.height, Round: tp.round}
	} else { // é precommit
		if tp.lockedVote != nil {
			vote.BlockID = tp.lockedVote.BlockID
		}
		votePayload = PrecommitPayload{Vote: *vote, Height: tp.height, Round: tp.round}
	}

	// Adiciona o próprio voto localmente.
	tp.votesMutex.Lock()
	if isPrevote {
		tp.prevotes[tp.node.ID] = vote
	} else {
		tp.precommits[tp.node.ID] = vote
	}
	tp.votesMutex.Unlock()

	tp.node.Network.Broadcast(tp.node.ID, votePayload)
}

// getQuorum verifica se há votos suficientes para um bloco específico ou para <nil>.
func (tp *TendermintProtocol) getQuorum(votes map[int]*Vote) (*int, bool) {
	// Esta função é chamada com votesMutex JÁ ADQUIRIDO (RLock).
	counts := make(map[int]int)
	nilVotes := 0

	for _, v := range votes {
		if v.BlockID != nil {
			counts[*v.BlockID]++
		} else {
			nilVotes++
		}
	}

	if nilVotes >= tp.quorumSize {
		return nil, true
	}

	for id, count := range counts {
		if count >= tp.quorumSize {
			blockID := id
			return &blockID, true
		}
	}

	return nil, false
}

// resetRoundState limpa o estado para uma nova rodada.
func (tp *TendermintProtocol) resetRoundState(keepLockedVote bool) {
	// Esta função é chamada com stateMutex JÁ ADQUIRIDO.
	tp.step = Propose
	tp.proposal = nil
	if !keepLockedVote {
		tp.lockedVote = nil
	}

	tp.votesMutex.Lock()
	// Otimização: em vez de recriar os mapas, apenas limpe-os.
	for k := range tp.prevotes {
		delete(tp.prevotes, k)
	}
	for k := range tp.precommits {
		delete(tp.precommits, k)
	}
	tp.votesMutex.Unlock()
}

// getProposer determina o líder da rodada.
func (tp *TendermintProtocol) getProposer(height, round int) int {
	return (height + round) % NumNodes
}

// getTimeout calcula o tempo de espera para uma fase, aumentando a cada rodada.
func (tp *TendermintProtocol) getTimeout(round int) float64 {
	return float64(TimeoutDeltaMs * round)
}

// log é uma função de logging unificada.
// *** CORREÇÃO: Removido o Lock/Unlock do Mutex daqui. ***
// A função que chama o log é responsável por garantir a segurança.
func (tp *TendermintProtocol) log(message string) {
	if !Debug {
		return
	}
	fmt.Printf("[%8.1fms] H:%d R:%d S:%-9s | Nó %d: %s\n",
		tp.sim.CurrentTime, tp.height, tp.round, tp.step, tp.node.ID, message)
}

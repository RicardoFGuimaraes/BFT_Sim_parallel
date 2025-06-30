// Arquivo: tendermint.go (Corrigido para Deadlock e Otimizado para Escalabilidade)
package main

import (
	"BFT-Simulator/simulator"
	"fmt"
	"sync"
)

// --- Tipos Específicos do Protocolo Tendermint ---
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

// --- Constantes de Timeout ---
const (
	TimeoutProposeMs   = 5000
	TimeoutPrevoteMs   = 5000
	TimeoutPrecommitMs = 5000
	TimeoutDeltaMs     = 1000
)

// --- Implementação do Protocolo Tendermint (Otimizada e Corrigida) ---
type TendermintProtocol struct {
	node       *simulator.Node
	quorumSize int

	// Otimização e Correção:
	// - stateMutex é um Mutex simples para evitar starvation do escritor.
	// - A ORDEM DE LOCK É SEMPRE: stateMutex -> votesMutex.
	stateMutex sync.Mutex   // Protege height, round, step, proposal, e lockedVote
	votesMutex sync.RWMutex // Protege os mapas de votos

	// Estado do Consenso
	height     int
	round      int
	step       TendermintStep
	proposal   *Block
	lockedVote *Vote

	// Otimização: Reutilização dos mapas para reduzir a pressão do GC.
	prevotes   map[int]*Vote
	precommits map[int]*Vote
}

func NewTendermintProtocol(node *simulator.Node) simulator.ConsensusProtocol {
	f := (NumNodes - 1) / 3
	return &TendermintProtocol{
		node:       node,
		quorumSize: 2*f + 1,
		height:     1,
		round:      0,
		prevotes:   make(map[int]*Vote),
		precommits: make(map[int]*Vote),
	}
}

func (tp *TendermintProtocol) Start() {
	if Debug {
		fmt.Printf("Nó %d iniciando protocolo.\n", tp.node.ID)
	}
	for {
		tp.enterNewRound()
	}
}

func (tp *TendermintProtocol) enterNewRound() {
	// Guarda o estado da rodada atual para verificações de timeout
	currentHeight, currentRound := tp.getCurrentState()

	// --- FASE PROPOSE ---
	tp.stateMutex.Lock()
	tp.step = Propose
	tp.resetRoundState()

	if Debug {
		tp.log(tp.height, tp.round, tp.step, "iniciando nova rodada.")
	}

	proposerID := tp.getProposer(tp.height, tp.round)
	if tp.node.ID == proposerID {
		var block Block
		if tp.lockedVote != nil && tp.lockedVote.BlockID != nil {
			block = Block{ID: *tp.lockedVote.BlockID}
		} else {
			block = Block{ID: tp.height*10000 + tp.round}
		}
		proposal := Proposal{Block: block, Height: tp.height, Round: tp.round}
		tp.proposal = &block
		if Debug {
			tp.log(tp.height, tp.round, tp.step, fmt.Sprintf("é o proponente, enviou proposta para bloco %d", block.ID))
		}
		tp.node.Network.Broadcast(tp.node.ID, proposal)
	}
	tp.stateMutex.Unlock()

	tp.node.Proc.Wait(tp.node.Proc.Timeout(tp.getTimeout(TimeoutProposeMs, currentRound)))
	if tp.hasStateChanged(currentHeight, currentRound) {
		return
	}

	// --- FASE PREVOTE ---
	tp.stateMutex.Lock()
	tp.step = Prevote
	tp.broadcastVote(true)
	tp.stateMutex.Unlock()

	tp.node.Proc.Wait(tp.node.Proc.Timeout(tp.getTimeout(TimeoutPrevoteMs, currentRound)))
	if tp.hasStateChanged(currentHeight, currentRound) {
		return
	}

	// --- FASE PRECOMMIT ---
	tp.stateMutex.Lock() // Adquire lock de estado para a transição atômica
	if !tp.isStateCurrent(currentHeight, currentRound) {
		tp.stateMutex.Unlock()
		return
	}

	tp.votesMutex.RLock()
	polkaBlockID, hasPolka := tp.getQuorum(tp.prevotes)
	tp.votesMutex.RUnlock()

	tp.step = Precommit
	if hasPolka {
		if Debug {
			logMsg := "formou Polka para <nil>"
			if polkaBlockID != nil {
				logMsg = fmt.Sprintf("formou Polka para bloco %d", *polkaBlockID)
			}
			tp.log(tp.height, tp.round, tp.step, logMsg)
		}
		tp.lockedVote = &Vote{BlockID: polkaBlockID}
	} else {
		if Debug {
			tp.log(tp.height, tp.round, tp.step, "não formou Polka, votando <nil>.")
		}
		tp.lockedVote = &Vote{BlockID: nil}
	}
	tp.broadcastVote(false)
	tp.stateMutex.Unlock()

	tp.node.Proc.Wait(tp.node.Proc.Timeout(tp.getTimeout(TimeoutPrecommitMs, currentRound)))
	if tp.hasStateChanged(currentHeight, currentRound) {
		return
	}

	// --- FASE COMMIT/NOVA RODADA ---
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()

	if !tp.isStateCurrent(currentHeight, currentRound) {
		return
	}

	tp.votesMutex.RLock()
	commitBlockID, hasCommit := tp.getQuorum(tp.precommits)
	tp.votesMutex.RUnlock()

	if hasCommit && commitBlockID != nil {
		tp.log(tp.height, tp.round, tp.step, fmt.Sprintf("BLOCO %d FINALIZADO! Notificando outros nós.", *commitBlockID))
		tp.node.Network.Broadcast(tp.node.ID, DecisionPayload{Height: tp.height})
		tp.height++
		tp.round = 0
	} else {
		if Debug {
			tp.log(tp.height, tp.round, tp.step, "timeout ou <nil> quorum, avançando para a próxima rodada.")
		}
		tp.round++
	}
}

func (tp *TendermintProtocol) HandleMessage(msg simulator.Message) {
	switch payload := msg.Payload.(type) {
	case Proposal:
		tp.stateMutex.Lock()
		if payload.Height == tp.height && payload.Round == tp.round && tp.step == Propose && tp.proposal == nil {
			tp.proposal = &payload.Block
			if Debug {
				tp.log(tp.height, tp.round, tp.step, fmt.Sprintf("recebeu proposta de %d para o bloco %d", msg.SenderID, payload.Block.ID))
			}
		}
		tp.stateMutex.Unlock()

	case PrevotePayload:
		tp.stateMutex.Lock()
		if payload.Height == tp.height && payload.Round == tp.round {
			tp.votesMutex.Lock()
			if _, exists := tp.prevotes[payload.Vote.VoterID]; !exists {
				tp.prevotes[payload.Vote.VoterID] = &payload.Vote
			}
			tp.votesMutex.Unlock()
		}
		tp.stateMutex.Unlock()

	case PrecommitPayload:
		tp.stateMutex.Lock()
		if payload.Height == tp.height && payload.Round == tp.round {
			tp.votesMutex.Lock()
			if _, exists := tp.precommits[payload.Vote.VoterID]; !exists {
				tp.precommits[payload.Vote.VoterID] = &payload.Vote
			}
			tp.votesMutex.Unlock()
		}
		tp.stateMutex.Unlock()

	case DecisionPayload:
		tp.stateMutex.Lock()
		if payload.Height >= tp.height {
			if Debug {
				tp.log(tp.height, tp.round, tp.step, fmt.Sprintf("recebeu notificação de decisão para altura %d. Sincronizando.", payload.Height))
			}
			tp.height = payload.Height + 1
			tp.round = 0
		}
		tp.stateMutex.Unlock()
	}
}

// --- Funções Auxiliares Otimizadas ---

func (tp *TendermintProtocol) broadcastVote(isPrevote bool) {
	// Esta função é chamada com stateMutex JÁ ADQUIRIDO
	var votePayload interface{}
	vote := &Vote{VoterID: tp.node.ID}

	currentHeight := tp.height
	currentRound := tp.round

	if isPrevote {
		if tp.proposal != nil {
			vote.BlockID = &tp.proposal.ID
		}
		votePayload = PrevotePayload{Vote: *vote, Height: currentHeight, Round: currentRound}
	} else {
		if tp.lockedVote != nil {
			vote.BlockID = tp.lockedVote.BlockID
		}
		votePayload = PrecommitPayload{Vote: *vote, Height: currentHeight, Round: currentRound}
	}

	// Adiciona o próprio voto localmente. A ordem de lock (state -> votes) é respeitada.
	tp.votesMutex.Lock()
	if isPrevote {
		tp.prevotes[tp.node.ID] = vote
	} else {
		tp.precommits[tp.node.ID] = vote
	}
	tp.votesMutex.Unlock()

	tp.node.Network.Broadcast(tp.node.ID, votePayload)
}

func (tp *TendermintProtocol) getQuorum(votes map[int]*Vote) (*int, bool) {
	// Esta função é chamada com votesMutex JÁ ADQUIRIDO (RLock)
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

func (tp *TendermintProtocol) resetRoundState() {
	// Esta função é chamada com stateMutex JÁ ADQUIRIDO
	tp.proposal = nil
	// tp.lockedVote é mantido entre rodadas

	tp.votesMutex.Lock()
	for k := range tp.prevotes {
		delete(tp.prevotes, k)
	}
	for k := range tp.precommits {
		delete(tp.precommits, k)
	}
	tp.votesMutex.Unlock()
}

func (tp *TendermintProtocol) getProposer(height, round int) int {
	return (height + round) % NumNodes
}

func (tp *TendermintProtocol) getTimeout(baseTimeout, round int) float64 {
	return float64(baseTimeout + (TimeoutDeltaMs * round))
}

func (tp *TendermintProtocol) getCurrentState() (int, int) {
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()
	return tp.height, tp.round
}

func (tp *TendermintProtocol) hasStateChanged(expectedHeight, expectedRound int) bool {
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()
	return tp.height != expectedHeight || tp.round != expectedRound
}

func (tp *TendermintProtocol) isStateCurrent(expectedHeight, expectedRound int) bool {
	// Esta função é chamada com stateMutex JÁ ADQUIRIDO
	return tp.height == expectedHeight && tp.round == expectedRound
}

func (tp *TendermintProtocol) log(h int, r int, s TendermintStep, message string) {
	// Chamada com stateMutex JÁ ADQUIRIDO
	fmt.Printf("[%8.1fms] H:%d R:%d S:%-9s | Nó %d: %s\n",
		tp.node.Proc.Now(), h, r, s, tp.node.ID, message)
}

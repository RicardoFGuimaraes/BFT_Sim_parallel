package main

import (
	"BFT-Simulator/simulator"
	"fmt"
	"sync"
)

// --- Tipos e Passos do Protocolo ---
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

// --- Constantes Dinâmicas e de Simulação ---
const (
	NodeProcessingDelayMs = 100.0
)

// --- Implementação do Protocolo Tendermint ---
type TendermintProtocol struct {
	node       *simulator.Node
	sim        *simulator.Simulation
	mc         *simulator.MetricsCollector
	quorumSize int

	timeoutPropose   float64
	timeoutPrevote   float64
	timeoutPrecommit float64
	timeoutDelta     float64

	stateMutex sync.Mutex
	votesMutex sync.RWMutex

	height     int
	round      int
	step       TendermintStep
	proposal   *Block
	lockedVote *Vote
	prevotes   map[int]*Vote
	precommits map[int]*Vote
}

// NewTendermintProtocol cria uma nova instância do protocolo.
func NewTendermintProtocol(node *simulator.Node, sim *simulator.Simulation, mc *simulator.MetricsCollector, numNodes int) simulator.ConsensusProtocol {
	f := (numNodes - 1) / 3
	baseTimeout := float64(3000) // Increased timeouts for stability
	delta := float64(500)

	return &TendermintProtocol{
		node:             node,
		sim:              sim,
		mc:               mc,
		quorumSize:       2*f + 1,
		timeoutPropose:   baseTimeout,
		timeoutPrevote:   baseTimeout,
		timeoutPrecommit: baseTimeout,
		timeoutDelta:     delta,
		height:           1,
		round:            0,
		step:             Propose,
		prevotes:         make(map[int]*Vote),
		precommits:       make(map[int]*Vote),
	}
}

// Start agenda o primeiro evento do nó.
func (tp *TendermintProtocol) Start() {
	tp.sim.Schedule(tp.enterNewHeight, 0)
}

// HandleMessage introduz um atraso e chama o processamento da mensagem.
func (tp *TendermintProtocol) HandleMessage(msg simulator.Message) {
	tp.sim.Schedule(func() {
		tp.processMessage(msg)
	}, NodeProcessingDelayMs)
}

// processMessage contém a lógica de tratamento de mensagens.
func (tp *TendermintProtocol) processMessage(msg simulator.Message) {
	tp.stateMutex.Lock()
	currentHeight := tp.height
	currentRound := tp.round
	currentStep := tp.step
	tp.stateMutex.Unlock()

	switch payload := msg.Payload.(type) {
	case Proposal:
		// We only care about proposals for the current height/round/step
		if payload.Height == currentHeight && payload.Round == currentRound && currentStep == Propose {
			tp.stateMutex.Lock()
			// Only accept the first valid proposal for this round
			if tp.proposal == nil {
				tp.proposal = &payload.Block
				tp.log(fmt.Sprintf("Recebeu proposta de %d para o bloco %d", msg.SenderID, payload.Block.ID))
				// *** REACTIVITY CHANGE ***
				// Instead of waiting for the timeout, immediately enter the Prevote step.
				tp.enterPrevote(currentHeight, currentRound)
			}
			tp.stateMutex.Unlock()
		}
	case PrevotePayload:
		if payload.Height == currentHeight && payload.Round == currentRound && currentStep == Prevote {
			added := false
			tp.votesMutex.Lock()
			if _, exists := tp.prevotes[payload.Vote.VoterID]; !exists {
				tp.prevotes[payload.Vote.VoterID] = &payload.Vote
				added = true
			}
			tp.votesMutex.Unlock()

			if added {
				// Attempt to advance as soon as a new vote comes in.
				tp.tryAdvanceToPrecommit(currentHeight, currentRound, false)
			}
		}
	case PrecommitPayload:
		if payload.Height == currentHeight && payload.Round == currentRound && currentStep == Precommit {
			added := false
			tp.votesMutex.Lock()
			if _, exists := tp.precommits[payload.Vote.VoterID]; !exists {
				tp.precommits[payload.Vote.VoterID] = &payload.Vote
				added = true
			}
			tp.votesMutex.Unlock()

			if added {
				// Attempt to decide as soon as a new vote comes in.
				tp.tryAdvanceToDecision(currentHeight, currentRound, false)
			}
		}
	case DecisionPayload:
		tp.stateMutex.Lock()
		if payload.Height >= tp.height {
			tp.log(fmt.Sprintf("Recebeu notificação de decisão para altura %d. Sincronizando.", payload.Height))
			tp.height = payload.Height + 1
			tp.sim.Schedule(tp.enterNewHeight, 1.0)
		}
		tp.stateMutex.Unlock()
	}
}

func (tp *TendermintProtocol) enterNewHeight() {
	tp.stateMutex.Lock()
	tp.log("Iniciando nova altura.")
	tp.round = 0
	tp.resetRoundState(false)
	tp.stateMutex.Unlock()
	tp.startRound()
}

func (tp *TendermintProtocol) startRound() {
	tp.stateMutex.Lock()
	currentHeight := tp.height
	currentRound := tp.round
	tp.step = Propose
	tp.log("Iniciando fase de Propose.")

	proposerID := tp.getProposer(tp.height, tp.round)
	if tp.node.ID == proposerID {
		var block Block
		if tp.lockedVote != nil && tp.lockedVote.BlockID != nil {
			block = Block{ID: *tp.lockedVote.BlockID}
		} else {
			// Unique block ID for each height/round combination
			block = Block{ID: tp.height*10000 + tp.round}
		}
		tp.mc.RecordProposal(block.ID, tp.sim.CurrentTime)
		proposal := Proposal{Block: block, Height: tp.height, Round: tp.round}
		// The proposal is set here for the proposer itself
		tp.proposal = &block
		tp.log(fmt.Sprintf("É o proponente, enviando proposta para bloco %d", block.ID))
		tp.node.Network.Broadcast(tp.node.ID, proposal)
	}
	tp.stateMutex.Unlock()

	// The timeout now acts as a fallback if the proposer is slow or the message is lost.
	timeout := tp.getTimeout(tp.timeoutPropose, currentRound)
	tp.sim.Schedule(func() { tp.onTimeoutPropose(currentHeight, currentRound) }, timeout)
}

// *** REACTIVITY CHANGE ***
// onTimeoutPropose now just triggers the transition to Prevote. The core logic is in enterPrevote.
func (tp *TendermintProtocol) onTimeoutPropose(expectedHeight, expectedRound int) {
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()

	// If we already progressed to the next step (e.g., by receiving a proposal), do nothing.
	if tp.height != expectedHeight || tp.round != expectedRound || tp.step != Propose {
		return
	}

	tp.log("Timeout de Propose, iniciando fase de Prevote.")
	// A proposal was not received in time.
	tp.enterPrevote(expectedHeight, expectedRound)
}

// *** REACTIVITY CHANGE ***
// This new function contains the logic to enter the prevote step.
// It can be called either by receiving a proposal or by a timeout.
func (tp *TendermintProtocol) enterPrevote(expectedHeight, expectedRound int) {
	// This function is now called with the lock held.
	// Ensure we are in the correct state to enter prevote.
	if tp.height != expectedHeight || tp.round != expectedRound || tp.step != Propose {
		return
	}

	tp.step = Prevote
	tp.broadcastVote(true) // Broadcast Prevote vote

	timeout := tp.getTimeout(tp.timeoutPrevote, expectedRound)
	// The timeout handler for prevote is tryAdvanceToPrecommit
	tp.sim.Schedule(func() { tp.tryAdvanceToPrecommit(expectedHeight, expectedRound, true) }, timeout)
}

func (tp *TendermintProtocol) tryAdvanceToPrecommit(expectedHeight, expectedRound int, isTimeout bool) {
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()

	if tp.height != expectedHeight || tp.round != expectedRound || tp.step != Prevote {
		return
	}

	tp.votesMutex.RLock()
	polkaBlockID, hasPolka := tp.getQuorum(tp.prevotes)
	tp.votesMutex.RUnlock()

	// If it's not a timeout, we only proceed if we actually have a polka.
	if !isTimeout && !hasPolka {
		return
	}

	tp.log("Avançando para a fase de Precommit.")
	tp.step = Precommit

	if hasPolka {
		logMsg := "formou Polka para <nil>"
		if polkaBlockID != nil {
			logMsg = fmt.Sprintf("formou Polka para bloco %d", *polkaBlockID)
		}
		tp.log(logMsg)
		tp.lockedVote = &Vote{BlockID: polkaBlockID}
	} else {
		// This case is now only reached on timeout
		tp.log("Timeout de Prevote sem Polka, votando <nil>.")
		tp.lockedVote = &Vote{BlockID: nil}
	}

	tp.broadcastVote(false) // Broadcast Precommit vote

	timeout := tp.getTimeout(tp.timeoutPrecommit, expectedRound)
	tp.sim.Schedule(func() { tp.tryAdvanceToDecision(expectedHeight, expectedRound, true) }, timeout)
}

func (tp *TendermintProtocol) tryAdvanceToDecision(expectedHeight, expectedRound int, isTimeout bool) {
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()

	if tp.height != expectedHeight || tp.round != expectedRound || tp.step != Precommit {
		return
	}

	tp.votesMutex.RLock()
	commitBlockID, hasCommitQuorum := tp.getQuorum(tp.precommits)
	tp.votesMutex.RUnlock()

	// If it's not a timeout, we only proceed if we have a commit for a specific block.
	if !isTimeout {
		if !hasCommitQuorum || commitBlockID == nil {
			return
		}
	}

	if hasCommitQuorum && commitBlockID != nil {
		tp.mc.RecordFinalization(*commitBlockID, tp.sim.CurrentTime)
		tp.log(fmt.Sprintf("BLOCO %d FINALIZADO! Notificando outros nós.", *commitBlockID))
		tp.node.Network.Broadcast(tp.node.ID, DecisionPayload{Height: tp.height})
		tp.height++
		tp.sim.Schedule(tp.enterNewHeight, 1.0)
	} else {
		// This case is now mostly for timeouts or nil-vote quorums.
		tp.log("Timeout ou <nil> quorum de Precommit, avançando para a próxima rodada.")
		tp.round++
		tp.resetRoundState(true) // Keep the locked vote for the next round
		tp.sim.Schedule(tp.startRound, 1.0)
	}
}

// --- Funções Auxiliares ---

func (tp *TendermintProtocol) broadcastVote(isPrevote bool) {
	var votePayload interface{}
	vote := &Vote{VoterID: tp.node.ID}

	if isPrevote {
		// Prevote for the proposal we received/have. Can be nil if timed out.
		if tp.proposal != nil {
			vote.BlockID = &tp.proposal.ID
		}
		votePayload = PrevotePayload{Vote: *vote, Height: tp.height, Round: tp.round}
	} else {
		// Precommit for our locked vote.
		if tp.lockedVote != nil {
			vote.BlockID = tp.lockedVote.BlockID
		}
		votePayload = PrecommitPayload{Vote: *vote, Height: tp.height, Round: tp.round}
	}

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

func (tp *TendermintProtocol) resetRoundState(keepLockedVote bool) {
	tp.proposal = nil
	if !keepLockedVote {
		tp.lockedVote = nil
	}

	tp.votesMutex.Lock()
	clear(tp.prevotes)
	clear(tp.precommits)
	tp.votesMutex.Unlock()
}

func (tp *TendermintProtocol) getProposer(height, round int) int {
	return (height + round) % NumNodes
}

func (tp *TendermintProtocol) getTimeout(baseTimeout float64, round int) float64 {
	return baseTimeout + (tp.timeoutDelta * float64(round))
}

func (tp *TendermintProtocol) log(message string) {
	if !Debug {
		return
	}
	fmt.Printf("[%8.1fms] H:%d R:%d S:%-9s | Nó %d: %s\n",
		tp.sim.CurrentTime, tp.height, tp.round, tp.step, tp.node.ID, message)
}

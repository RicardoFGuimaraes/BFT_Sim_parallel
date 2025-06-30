package simulator

// Message é uma estrutura genérica para mensagens trocadas na rede.
// O Payload é uma interface vazia para acomodar diferentes tipos de mensagens de protocolo.
type Message struct {
	SenderID   int
	ReceiverID int // -1 para broadcast
	Payload    interface{}
}

type QuorumCertificate struct {
	View    int
	BlockID int
}

type Block struct {
	ID int
}

// HSProposal é a proposta do líder, justificada por um QC.
type HSProposal struct {
	Block   Block
	View    int
	Justify *QuorumCertificate
}

// HSVote é o voto de uma réplica para o líder.
type HSVote struct {
	VoterID int
	BlockID int
	View    int
	Phase   HotStuffPhase
}

type HotStuffPhase int

func (p HotStuffPhase) String() string {
	return [...]string{"Prepare", "PreCommit", "Commit", "Decide"}[p]
}

package simulator

import (
	"container/heap"
	"runtime"
	"time"
)

// Event define uma ação agendada na simulação. (Sem alterações)
type Event struct {
	Timestamp float64
	Handler   func()
	index     int
}

// EventQueue é uma fila de prioridades para eventos. (Sem alterações)
type EventQueue []*Event

func (eq EventQueue) Len() int           { return len(eq) }
func (eq EventQueue) Less(i, j int) bool { return eq[i].Timestamp < eq[j].Timestamp }
func (eq EventQueue) Swap(i, j int)      { eq[i], eq[j] = eq[j], eq[i]; eq[i].index = i; eq[j].index = j }
func (eq *EventQueue) Push(x interface{}) {
	n := len(*eq)
	event := x.(*Event)
	event.index = n
	*eq = append(*eq, event)
}
func (eq *EventQueue) Pop() interface{} {
	old := *eq
	n := len(old)
	event := old[n-1]
	old[n-1] = nil
	event.index = -1
	*eq = old[0 : n-1]
	return event
}

// Simulation gerencia o estado e a execução da simulação.
type Simulation struct {
	CurrentTime float64
	eventQueue  EventQueue
}

func NewSimulation() *Simulation {
	return &Simulation{
		CurrentTime: 0,
		eventQueue:  make(EventQueue, 0),
	}
}

// Schedule adiciona um evento à fila para execução futura.
func (s *Simulation) Schedule(handler func(), delay float64) {
	if delay < 0 {
		delay = 0
	}
	event := &Event{
		Timestamp: s.CurrentTime + delay,
		Handler:   handler,
	}
	heap.Push(&s.eventQueue, event)
}

// RunUntil executa a simulação até um determinado tempo limite.
// *** LÓGICA CORRIGIDA ***
func (s *Simulation) RunUntil(limit float64) {
	for {
		// Se a fila de eventos está vazia, há duas possibilidades:
		// 1. A simulação realmente terminou.
		// 2. As goroutines dos nós estão processando e irão agendar novos eventos.
		if len(s.eventQueue) == 0 {
			// Se o tempo já passou do limite, podemos sair com segurança.
			if s.CurrentTime >= limit {
				break
			}
			// Damos uma chance para outras goroutines (os nós) rodarem e agendarem eventos.
			// time.Sleep é uma forma de fazer isso, mas runtime.Gosched() é mais apropriado.
			runtime.Gosched()
			// Pequeno sleep para evitar busy-waiting agressivo se não houver eventos por um tempo.
			time.Sleep(1 * time.Millisecond)
			continue
		}

		// Pega o próximo evento da fila.
		nextEvent := s.eventQueue[0] // Espia o evento sem removê-lo

		// Se o próximo evento está além do nosso limite de tempo, paramos.
		if nextEvent.Timestamp > limit {
			break
		}

		// Remove o evento e o processa.
		event := heap.Pop(&s.eventQueue).(*Event)
		s.CurrentTime = event.Timestamp
		event.Handler()
	}
}

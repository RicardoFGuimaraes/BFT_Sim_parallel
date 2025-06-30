package simulator

import (
	"container/heap"
	"runtime"
	"sync"
	"time"
)

// Event e EventQueue não mudam
type Event struct {
	Timestamp float64
	Handler   func()
	index     int
}
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

// Simulation agora inclui um Mutex para acesso seguro à fila.
type Simulation struct {
	CurrentTime float64
	eventQueue  EventQueue
	mutex       sync.Mutex // Adicionado Mutex para thread-safety
}

func NewSimulation() *Simulation {
	return &Simulation{
		CurrentTime: 0,
		eventQueue:  make(EventQueue, 0),
	}
}

// Schedule agora é "thread-safe".
func (s *Simulation) Schedule(handler func(), delay float64) {
	// Bloqueia o mutex para garantir acesso exclusivo à fila.
	s.mutex.Lock()
	defer s.mutex.Unlock() // Garante que o mutex será liberado ao sair da função.

	if delay < 0 {
		delay = 0
	}
	event := &Event{
		Timestamp: s.CurrentTime + delay,
		Handler:   handler,
	}
	heap.Push(&s.eventQueue, event)
}

// RunUntil foi refatorado para usar o mutex e evitar condições de corrida.
func (s *Simulation) RunUntil(limit float64) {
	for {
		s.mutex.Lock() // Bloqueia o acesso no início de cada iteração.

		// Se a fila estiver vazia, precisamos liberar o lock e esperar.
		if len(s.eventQueue) == 0 {
			s.mutex.Unlock() // Libera para que outras goroutines possam agendar eventos.

			// Se já passamos do tempo, podemos sair.
			if s.CurrentTime >= limit {
				break
			}

			// Cede o processador para outras goroutines.
			runtime.Gosched()
			// Um pequeno sleep evita que este loop consuma 100% da CPU se não houver eventos.
			time.Sleep(1 * time.Millisecond)
			continue // Volta para o início do loop.
		}

		// Espia o próximo evento (com o lock ainda ativo).
		nextEvent := s.eventQueue[0]

		// Se o próximo evento está além do limite de tempo, liberamos o lock e saímos.
		if nextEvent.Timestamp > limit {
			s.mutex.Unlock()
			break
		}

		// Remove o evento da fila de forma atômica.
		event := heap.Pop(&s.eventQueue).(*Event)
		s.CurrentTime = event.Timestamp

		// Importante: Liberamos o lock ANTES de executar o handler.
		// Isso evita deadlocks caso o handler tente chamar Schedule() novamente.
		s.mutex.Unlock()

		event.Handler()
	}
}

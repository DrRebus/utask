package step

import "slices"

// possible states of a step
const (
	StateAny           = "ANY" // wildcard
	StateTODO          = "TODO"
	StateWaiting       = "WAITING"
	StateRunning       = "RUNNING"
	StateDone          = "DONE"
	StateClientError   = "CLIENT_ERROR"
	StateServerError   = "SERVER_ERROR"
	StateFatalError    = "FATAL_ERROR"
	StateCrashed       = "CRASHED"
	StatePrune         = "PRUNE"
	StateToRetry       = "TO_RETRY"
	StateRetryNow      = "RETRY_NOW"
	StateAfterrunError = "AFTERRUN_ERROR"

	// steps that carry a foreach list of arguments
	StateExpanded = "EXPANDED"
)

var (
	BuiltinStates            = StepStateList{StateTODO, StateWaiting, StateRunning, StateDone, StateClientError, StateServerError, StateFatalError, StateCrashed, StatePrune, StateToRetry, StateRetryNow, StateAfterrunError, StateAny, StateExpanded}
	StepConditionValidStates = StepStateList{StateDone, StatePrune, StateToRetry, StateRetryNow, StateFatalError, StateClientError}
	RunnableStates           = StepStateList{StateTODO, StateServerError, StateClientError, StateFatalError, StateCrashed, StateToRetry, StateRetryNow, StateAfterrunError, StateExpanded, StateWaiting} // everything but RUNNING, DONE, PRUNE
	RetriableStates          = StepStateList{StateServerError, StateToRetry, StateAfterrunError}
	ValidAfterRunStates      = StepStateList{StateDone, StateClientError, StateAfterrunError}
)

type StepStateList []string

func (s StepStateList) Contains(state string) bool {
	return slices.Contains(s, state)
}

package step

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"

	"github.com/ovh/utask"
	"github.com/ovh/utask/engine/functions"
	"github.com/ovh/utask/engine/step/condition"
	"github.com/ovh/utask/engine/step/executor"
	"github.com/ovh/utask/engine/values"
	"github.com/ovh/utask/models/step"
	"github.com/ovh/utask/pkg/jsonschema"
	"github.com/ovh/utask/pkg/utils"
)

// retry patterns for a step
const (
	RetrySeconds = "seconds"
	RetryMinutes = "minutes"
	RetryHours   = "hours"

	ForEachStrategyParallel = "parallel"
	ForEachStrategySequence = "sequence"
)

// possible states of a step
const (
	StateAny           = step.StateAny
	StateTODO          = step.StateTODO
	StateWaiting       = step.StateWaiting
	StateRunning       = step.StateRunning
	StateDone          = step.StateDone
	StateClientError   = step.StateClientError
	StateServerError   = step.StateServerError
	StateFatalError    = step.StateFatalError
	StateCrashed       = step.StateCrashed
	StatePrune         = step.StatePrune
	StateToRetry       = step.StateToRetry
	StateRetryNow      = step.StateRetryNow
	StateAfterrunError = step.StateAfterrunError
	StateExpanded      = step.StateExpanded
)

const (
	stepRefThis = utask.This

	defaultMaxRetries = 10000

	maxExecutionDelay = time.Duration(20) * time.Second
)

type DBModel = step.DBModel
type EncryptedStepData = step.StepData
type Step = step.Step

// Context provides a step with extra metadata about the task
type Context struct {
	RequesterUsername string    `json:"requester_username"`
	ResolverUsername  string    `json:"resolver_username"`
	TaskID            string    `json:"task_id"`
	Created           time.Time `json:"created"`
}

////

func noopStep(st *Step, stepChan chan<- *Step) {
	stepChan <- st
}

func uniqueSortedList(s []string) []string {
	m := make(map[string]struct{})
	for _, str := range s {
		m[str] = struct{}{}
	}

	ret := make([]string, 0, len(m))
	for k := range m {
		ret = append(ret, k)
	}
	sort.Strings(ret)
	return ret
}

type execution struct {
	baseCfgRaw  json.RawMessage
	outputs     []*executor.Output
	config      json.RawMessage
	runner      Runner
	ctx         interface{}
	shutdownCtx context.Context
}

func (e *execution) generateOutput(st *Step, v *values.Values) error {
	for _, output := range e.outputs {
		switch output.Strategy {
		case executor.OutputStrategymerge:

			outputMap, ok := output.Format.(map[string]interface{})
			if !ok {
				st.Output = output.Format
				return errors.New("invalid output merge data format")
			}

			if st.Output == nil {
				st.Output = output.Format
				continue
			}

			marshaled, err := utils.JSONMarshal(st.Output)
			if err == nil {
				_ = utils.JSONnumberUnmarshal(bytes.NewReader(marshaled), &outputMap)
				st.Output = outputMap
			}

		case executor.OutputStrategytemplate:
			jsonOutput, err := utils.JSONMarshal(output.Format)
			if err != nil {
				return err
			}

			v.SetOutput(st.Name, st.Output)
			st.Output, err = rawResolveObject(v, jsonOutput, st.Item, st.Name)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func generateExecution(st *Step, action executor.Executor, baseConfig map[string]json.RawMessage, values *values.Values, shutdownCtx context.Context) (*execution, error) {
	var ret = execution{
		config:      action.Configuration,
		shutdownCtx: shutdownCtx,
	}
	var err error

	if action.BaseConfiguration != "" {
		base, ok := baseConfig[action.BaseConfiguration]
		if !ok {
			return nil, fmt.Errorf("could not find base configuration '%s'", action.BaseConfiguration)
		}

		resolvedBase, err := resolveObject(values, base, st.Item, st.Name)
		if err != nil {
			return nil, errors.Annotate(err, "failed to template base configuration")
		}
		ret.baseCfgRaw = resolvedBase
	}

	for { // until we break because no more functions

		if action.Output != nil && action.Output.Strategy != executor.OutputStrategynone {
			var skipAdd bool

			if action.Output.Strategy == executor.OutputStrategymerge {
				// XXX: Backward compatibility
				content, ok := action.Output.Format.(json.RawMessage)

				if !ok {
					content, err = utils.JSONMarshal(action.Output.Format)
					if err != nil {
						return nil, err
					}
				}

				output, err := rawResolveObject(values, content, st.Item, st.Name)
				if err != nil {
					return nil, errors.Annotate(err, "failed to template base output")
				}
				if output != nil {
					var ok bool
					outputMap, ok := output.(map[string]interface{})
					if !ok {
						return nil, errors.Annotate(errors.New("merge output not a map"), "failed to template merge output")
					}
					action.Output.Format = outputMap
				} else {
					skipAdd = true
				}
			}

			if !skipAdd {
				ret.outputs = append([]*executor.Output{action.Output}, ret.outputs...)
			}
		}

		ret.config, err = resolveObject(values, ret.config, st.Item, st.Name)
		if err != nil {
			return nil, errors.Annotate(err, "failed to template configuration")
		}

		ret.runner, err = getRunner(action.Type)
		if err != nil {
			return nil, err
		}

		// Check if we have a function as runner or not. If not, we do not need to go further in the resolution
		functionRunner, ok := ret.runner.(*functions.Function)
		if !ok {
			break
		}
		var functionInput map[string]interface{}
		if err := utils.JSONnumberUnmarshal(bytes.NewBuffer(ret.config), &functionInput); err != nil {
			return nil, errors.Annotate(err, "failed to template configuration")
		}

		values.SetFunctionsArgs(functionInput)
		ret.config = functionRunner.Action.Configuration
		action = functionRunner.Action
	}

	ret.ctx = ret.runner.Context(st.Name)
	if ret.ctx != nil {
		ctxMarshal, err := utils.JSONMarshal(ret.ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal context: %s", err)
		}
		ctxTmpl, err := values.Apply(string(ctxMarshal), st.Item, st.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to template context: %s", err)
		}
		err = utils.JSONnumberUnmarshal(bytes.NewReader(ctxTmpl), &ret.ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to re-marshal context: %s", err)
		}
	}

	return &ret, nil
}

func execute(st *Step, execution *execution, callback func(interface{}, interface{}, map[string]string, error)) {

	select {
	case <-execution.shutdownCtx.Done():
		st.State = StateToRetry
		return
	default:
		break
	}

	resources := append(execution.runner.Resources(execution.baseCfgRaw, execution.config), st.Resources...)
	limits := uniqueSortedList(resources)
	if acquiredErr := utask.AcquireResources(execution.shutdownCtx, limits); acquiredErr != nil {
		// if resource acquisition takes too long (timeout or shutdown), let's put the step in ToRetry state
		// to release the Execution pool, or let the instance shutdowns correctly, as the step execution didn't started yet
		callback(`{}`, "", map[string]string{}, errors.NotProvisionedf("failed to acquire resources"))
		return
	}
	defer utask.ReleaseResources(limits)

	output, metadata, tags, err := execution.runner.Exec(st.Name, execution.baseCfgRaw, execution.config, execution.ctx)
	callback(output, metadata, tags, err)
}

// Run carries out the action defined by a Step, by providing values to its configuration
// - a stepChan channel is provided for committing the result back
// - a shutdownCtx context is provided to interrupt execution in flight
// values IS NOT CONCURRENT SAFE, DO NOT SHARE WITH OTHER GOROUTINES
func Run(st *Step, baseConfig map[string]json.RawMessage, stepValues *values.Values, stepChan chan<- *Step, wg *sync.WaitGroup, shutdownCtx context.Context) {

	// Step already ran, directly going to afterrun process
	if st.State == StateAfterrunError {
		go noopStep(st, stepChan)
		return
	}

	if st.MaxRetries == 0 {
		st.MaxRetries = defaultMaxRetries
	}

	// we can set "max_retries" to a negative number to have full control
	// over the repetition of a step with a check condition using RETRY_NOW
	if st.MaxRetries > 0 && st.TryCount > st.MaxRetries {
		st.State = StateFatalError
		st.Error = fmt.Sprintf("Step reached max retries %d: %s", st.MaxRetries, st.Error)
		go noopStep(st, stepChan)
		return
	}

	if st.Skipped {
		go noopStep(st, stepChan)
		return
	}

	prehook, err := GetPreHook(st)
	if err != nil {
		st.State = StateFatalError
		st.Error = err.Error()
		go noopStep(st, stepChan)
		return
	}

	preHookValues, err := stepValues.Clone()
	if err != nil {
		st.State = StateFatalError
		st.Error = err.Error()
		go noopStep(st, stepChan)
		return
	}

	var prehookFailed bool
	var preHookWg sync.WaitGroup
	if prehook != nil {
		preHookExecution, err := generateExecution(st, *prehook, baseConfig, stepValues, shutdownCtx)
		if err != nil {
			st.State = StateFatalError
			st.Error = fmt.Sprintf("prehook: %s", err)
			go noopStep(st, stepChan)
			return
		}

		preHookWg.Add(1)
		go func() {
			defer preHookWg.Done()

			execute(st, preHookExecution, func(output interface{}, metadata interface{}, tags map[string]string, err error) {
				if err != nil {
					prehookFailed = true
					st.State = StateFatalError
					st.Error = fmt.Sprintf("prehook: %s", err)
					go noopStep(st, stepChan)
					return
				}
				preHookValues.SetPreHook(output, metadata)
			})
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		time.Sleep(st.ExecutionDelay)

		// Wait the pre-hook execution is done
		preHookWg.Wait()

		// Stop the Step execution if the pre-hook has failed.
		// Don't send the step into stepChan, because the pre_hook already did it.
		if prehookFailed {
			return
		}

		// After pre-hook execution, let's verify if we are not shutting down the instance
		// in that case, we can just put the step in ToRetry instead of starting the main execution of the step
		select {
		case <-shutdownCtx.Done():
			st.State = StateToRetry
			go noopStep(st, stepChan)
			return
		default:
		}

		// Generate the execution
		execution, err := generateExecution(st, st.Action, baseConfig, preHookValues, shutdownCtx)
		if err != nil {
			st.State = StateFatalError
			st.Error = err.Error()
			go noopStep(st, stepChan)
			return
		}

		execute(st, execution, func(output interface{}, metadata interface{}, tags map[string]string, err error) {
			st.Output, st.Metadata, st.Tags = output, metadata, tags

			outputErr := execution.generateOutput(st, preHookValues)
			if outputErr != nil {
				st.State = StateFatalError
				st.Error = "unable to format output: " + outputErr.Error()
			} else {
				if err != nil {
					if errors.IsBadRequest(err) {
						st.State = StateClientError
					} else if errors.IsNotAssigned(err) {
						st.State = StateWaiting
					} else if errors.IsNotProvisioned(err) {
						st.State = StateToRetry
					} else {
						st.State = StateServerError
					}
					st.Error = err.Error()
				} else if st.ResultValidate != nil {
					if err := st.ResultValidate(st.Output); err != nil {
						st.Error = err.Error()
						st.State = StateFatalError
					}
				}
				if _, err := utils.JSONMarshal(st.Output); err != nil {
					st.Error = "plugin output can't be json.Marshal: " + err.Error()
					st.State = StateFatalError
					st.Output = fmt.Sprint(st.Output)
				}

				if st.State == StateRunning {
					st.State = StateDone
					st.Error = ""
				}
			}

			st.TryCount++
		})

		stepChan <- st
	}()
}

// StateSetter is a handle to apply the effects of a condition evaluation
type StateSetter func(step, state, message string)

// PreRun evaluates a step's "skip" conditions before the Step's action has been performed
// and impacts the entire task's execution flow through the provided StateSetter
func PreRun(st *Step, values *values.Values, ss StateSetter, executedSteps map[string]bool) {
	conditions, err := GetConditions(st)
	if err != nil {
		ss(st.Name, StateServerError, err.Error())
		return
	}

	for _, sc := range conditions {
		if sc.Type != condition.SKIP {
			continue
		}
		if st.ForEach != "" && sc.ForEach != condition.ForEachParent {
			continue
		}

		if err := sc.Eval(values, st.Item, st.Name); err != nil {
			if _, ok := err.(condition.ErrConditionNotMet); ok {
				logrus.Debugf("PreRun: Step [%s] condition eval: %s", st.Name, err)
				continue
			} else { // Templating / strconv errors
				// Putting the step in SERVER_ERROR makes the resolution collectable by the RetryCollector.
				ss(st.Name, StateServerError, err.Error())

				// Do not run the step.
				st.Skipped = true
				// inserting current skipped step into executedSteps to avoid being picked-up again in availableSteps candidates
				executedSteps[st.Name] = true
				break
			}
		}

		// Reaching this means the condition is met: set the step to skipped
		st.Skipped = true

		// Inserting current skipped step into executedSteps to avoid being picked-up again in availableSteps candidates
		executedSteps[st.Name] = true
		for step, state := range sc.Then {
			if step == stepRefThis {
				step = st.Name
			}
			ss(step, state, sc.Message)
		}

		// If the condition is final, don't continue
		if sc.Final {
			break
		}
	}
}

// AfterRun evaluates a step's "check" conditions after the Step's action has been performed
// and impacts the entire task's execution flow through the provided StateSetter
func AfterRun(st *Step, values *values.Values, ss StateSetter) {
	// Not all steps' states should trigger evaluation of AfterRun conditions
	if st.Skipped || st.ForEach != "" || !step.ValidAfterRunStates.Contains(st.State) {
		return
	}

	conditions, err := GetConditions(st)
	if err != nil {
		ss(st.Name, StateServerError, err.Error())
		return
	}

	for _, sc := range conditions {
		if sc.Type != condition.CHECK {
			continue
		}
		if err := sc.Eval(values, st.Item, st.Name); err != nil {
			if _, ok := err.(condition.ErrConditionNotMet); ok {
				logrus.Debugf("AfterRun: Step [%s] condition eval: %s", st.Name, err)
				continue
			} else { // Templating / strconv errors
				// Putting the step in AFTERRUN_ERROR makes the resolution collectable (like step.PreRun),
				// but will skip the nexts step.Run, jumping directly to the after-run logic.
				ss(st.Name, StateAfterrunError, err.Error())
				break
			}
		}

		// Reaching this means the condition is met
		for step, state := range sc.Then {
			if step == stepRefThis {
				step = st.Name
			}
			ss(step, state, sc.Message)
		}

		// If the condition is final, don't continue
		if sc.Final {
			break
		}
	}
}

// ValidAndNormalize asserts that a step carries correct configuration
// - checks that executor is registered
// - validates retry pattern
// - validates custom states for the step (no collisions with builtin states)
// - validates conditions
// - validates the provided json schema for result validation
// - checks dependency declaration against the task's execution tree
func ValidAndNormalize(st *Step, name string, baseConfigs map[string]json.RawMessage, steps map[string]*Step) error {
	if name == stepRefThis {
		return errors.BadRequestf("'%s' step name is reserved", stepRefThis)
	}

	// valid action executor
	if _, err := validExecutor(baseConfigs, st.Action, st.PreHook); err != nil {
		return errors.NewNotValid(err, "Invalid executor action")
	}
	preHook, err := GetPreHook(st)
	if err != nil {
		return errors.NewNotValid(err, "Invalid prehook action")
	}
	if preHook != nil {
		ph, err := validExecutor(baseConfigs, *preHook, nil)
		if ph != nil {
			return errors.NewNotValid(nil, "Invalid prehook on a prehook")
		}
		if err != nil {
			return errors.NewNotValid(err, "Invalid prehook action")
		}
	}

	if st.ForEachStrategy != "" && st.ForEach == "" {
		return errors.NewNotValid(nil, "step foreach_strategy can't be set without foreach")
	}

	if st.ForEach != "" {
		switch st.ForEachStrategy {
		case ForEachStrategyParallel, ForEachStrategySequence:
		case "":
			// expliciting default value
			st.ForEachStrategy = ForEachStrategyParallel
		default:
			return errors.NewNotValid(nil, fmt.Sprintf("step foreach_strategy %q is not a valid value", st.ForEachStrategy))
		}
	}

	// valid execution delay
	if st.ExecutionDelay < 0 || st.ExecutionDelay > maxExecutionDelay {
		return errors.NewNotValid(nil,
			fmt.Sprintf("execution_delay: expected %s to be a duration between 0s and %s",
				st.ExecutionDelay, maxExecutionDelay))
	}

	// valid retry pattern, accept empty
	switch st.RetryPattern {
	case "", RetrySeconds, RetryMinutes, RetryHours:
	default:
		return errors.BadRequestf("Invalid retry pattern: %s Expecting(%s|%s|%s)", st.RetryPattern, RetrySeconds, RetryMinutes, RetryHours)
	}

	// valid custom states
	for _, cState := range st.CustomStates {
		if step.BuiltinStates.Contains(cState) {
			return errors.NewNotValid(nil,
				fmt.Sprintf(`Custom state %q is not allowed as it's a reserved state. Reserved state are: "%s"`,
					cState, strings.Join(step.BuiltinStates, `", "`)))
		}
	}

	// valid step conditions
	for _, sc := range st.Conditions {
		if st.ForEach != "" && sc.Type == condition.SKIP && sc.ForEach == "" {
			sc.ForEach = condition.ForEachChildren
		}
		if err := ValidCondition(sc, name, steps); err != nil {
			return err
		}
	}

	// normalize and validate json schema
	schema, err := jsonschema.NormalizeAndCompile(name, st.Schema)
	if err != nil {
		return errors.Annotatef(err, "Jsonschema: step %s", name)
	}
	st.Schema = schema

	// valid dependencies
	seenDependencies := map[string]bool{}
	for _, d := range st.Dependencies {
		// no orphan dependencies,
		depStep, depState := DependencyParts(d)
		s, ok := steps[depStep]
		if !ok {
			return errors.BadRequestf("Invalid dependency, no step with that name: %q", depStep)
		}
		if _, ok := seenDependencies[depStep]; ok {
			return errors.BadRequestf("Invalid dependency, already defined dependency to: %q", depStep)
		}
		if duplicated := utils.HasDupsArray(depState); duplicated {
			return errors.BadRequestf("Invalid dependency, duplicated state detected")
		}
		for _, state := range depState {
			switch state {
			case StateDone:
			case StateAny:
				if len(depState) != 1 {
					return errors.BadRequestf("Invalid dependency, no other state allowed if ANY is declared")
				}
			default:
				if !utils.ListContainsString(s.CustomStates, state) {
					return errors.BadRequestf("Invalid dependency on step %s, step state not allowed: %q", depStep, state)
				}
			}
		}
		seenDependencies[depStep] = true
	}

	// no circular dependencies,
	sourceChain := dependenciesChain(steps, st.Dependencies)
	if utils.ListContainsString(sourceChain, name) {
		sort.Strings(sourceChain)
		return errors.BadRequestf("Invalid: circular dependency %v <-> %s", sourceChain, name)
	}

	return nil
}

// ExecutorMetadata returns the step's runner metadata schema
func ExecutorMetadata(st *Step) json.RawMessage {
	runner, err := getRunner(st.Action.Type)
	if err != nil {
		return []byte{}
	}

	return runner.MetadataSchema()
}

func walkThroughFunctions(st *Step, f func(*functions.Function)) error {
	var runnerName = st.Action.Type
	for {
		runner, err := getRunner(runnerName)
		if err != nil {
			return err
		}

		functionRunner, ok := runner.(*functions.Function)
		if !ok {
			break
		}
		f(functionRunner)
		runnerName = functionRunner.Action.Type
	}
	return nil
}

// GetConditions returns the list of conditions of this step resolved (functions included)
func GetConditions(st *Step) ([]*condition.Condition, error) {
	conditions := st.Conditions

	if err := walkThroughFunctions(st, func(functionRunner *functions.Function) {
		conditions = append(functionRunner.Conditions, conditions...)
	}); err != nil {
		return nil, err
	}
	return conditions, nil
}

// GetCustomStates returns the list of custom states of the Step (functions included)
func GetCustomStates(st *Step) ([]string, error) {
	states := st.CustomStates

	if err := walkThroughFunctions(st, func(functionRunner *functions.Function) {
		states = utils.AppendUniq(states, functionRunner.CustomStates...)
	}); err != nil {
		return nil, err
	}
	return states, nil
}

// GetPreHook returns the prehook that need to be executed (function included)
func GetPreHook(st *Step) (*executor.Executor, error) {
	preHook := st.PreHook

	if err := walkThroughFunctions(st, func(functionRunner *functions.Function) {
		if functionRunner.PreHook != nil {
			preHook = functionRunner.PreHook
		}
	}); err != nil {
		return nil, err
	}
	return preHook, nil

}

func CheckIfValidState(st *Step) (bool, error) {
	if step.BuiltinStates.Contains(st.State) {
		return true, nil
	}

	states, err := GetCustomStates(st)
	if err != nil {
		return false, err
	}

	for _, state := range states {
		if state == st.State {
			return true, nil
		}
	}

	return false, nil
}

func annotateFunctions(functions []string, err error) error {
	for _, function := range functions {
		err = errors.Annotate(err, fmt.Sprintf("function %q", function))
	}
	return err
}

func validExecutor(baseConfigs map[string]json.RawMessage, ex executor.Executor, preHook *executor.Executor) (*executor.Executor, error) {
	if len(ex.BaseConfiguration) > 0 {
		if _, ok := baseConfigs[ex.BaseConfiguration]; !ok {
			return nil, errors.New("BaseConfiguration key not found")
		}
	}

	runnerType := ex.Type
	configuration := ex.Configuration
	var functionNames []string
	for {
		r, err := getRunner(runnerType)
		if err != nil {
			return nil, err
		}

		err = r.ValidConfig(baseConfigs[ex.BaseConfiguration], configuration)
		if err != nil {
			return nil, annotateFunctions(functionNames, err)
		}

		functionRunner, ok := r.(*functions.Function)
		if !ok {
			break
		}
		if utils.ListContainsString(functionNames, runnerType) {
			return nil, annotateFunctions(functionNames, fmt.Errorf("invalid cyclic import in function for %q", runnerType))
		}

		functionNames = append(functionNames, runnerType)

		runnerType, configuration = functionRunner.Action.Type, functionRunner.Action.Configuration
		if functionRunner.PreHook != nil {
			if preHook != nil {
				return functionRunner.PreHook, annotateFunctions(functionNames, errors.New("pre_hook override"))
			}
			preHook = functionRunner.PreHook
		}
	}

	return preHook, nil
}

// DependencyParts de-composes a Step's dependency into its constituent parts: step name + step state
// a dependency expressed only as a step name is equivalent to depending on that step being in DONE state
func DependencyParts(dep string) (string, []string) {
	var depStep string
	var depState []string
	parts := strings.SplitN(dep, ":", 2)
	depStep = parts[0]
	if len(parts) == 1 {
		depState = []string{StateDone}
	} else {
		depState = strings.Split(parts[1], ",")
	}
	return depStep, depState
}

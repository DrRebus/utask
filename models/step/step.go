package step

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/go-gorp/gorp"
	"github.com/juju/errors"
	"github.com/loopfz/gadgeto/zesty"
	"github.com/ovh/utask/db/pgjuju"
	"github.com/ovh/utask/db/sqlgenerator"
	"github.com/ovh/utask/engine/step/condition"
	"github.com/ovh/utask/engine/step/executor"
	"github.com/ovh/utask/models"
	"github.com/ovh/utask/pkg/compress"
	"github.com/ovh/utask/pkg/jsonschema"
	"github.com/ovh/utask/pkg/utils"
)

// Step describes one unit of work within a task, and its dependency to other steps
// a step contains an action that makes use of an available executor, with a specific parameter set
// The result of a step is stored as its output, and can be validated with json schema
// Any error and metadata returned by the step's executor will also be stored, resulting in a state
// The state of a step can be customized by the author of a template, to account for business-specific
// outcomes (eg. a 404 needn't be an error, it can be called NOT_FOUND and determine execution flow
// without blocking).
// Through the "foreach" parameter, a step can be configured to spawn sub-steps for a list of items:
// the result of such a step will be the collection of results of all sub-steps, which can be fed
// into another "foreach" step
// A step can be configured to evaluate "conditions" before and after the action is performed:
//   - a "skip" condition will be run before and might determine that the step's action can be skipped entirely
//   - a "check" condition will be run after the action, and can control execution flow by examining
//     the step's result and modifying step states through the entire task's resolution
type Step struct {
	DBModel
	Skipped        bool                    `json:"-" db:"-"`
	ResultValidate jsonschema.ValidateFunc `json:"-" db:"-"`
	CompressionAlg string                  `json:"-" db:"-"`
}

type DBModel struct {
	StepData
	ID                uint64    `json:"-" db:"id"`
	ResolutionID      uint64    `json:"-" db:"resolution_id"`
	Name              string    `json:"name" db:"name"`
	Description       string    `json:"description" db:"description"`
	State             string    `json:"state,omitempty" db:"state"`
	TryCount          int       `json:"try_count,omitempty" db:"try_count"`
	MaxRetries        int       `json:"max_retries,omitempty" db:"max_retries"`
	LastRun           time.Time `json:"last_run,omitempty" db:"last_run"`
	Idempotent        bool      `json:"idempotent" db:"idempotent"`
	EncryptedStepData string    `json:"-" db:"encrypted_step_data"`
}

// StepData holds all fields not needed to filter/search steps in the database. These fields are stored as a single
// compressed and encrypted blob.
type StepData struct {
	Tags            map[string]string `json:"tags" db:"-"`
	ChildrenSteps   []string          `json:"children_steps,omitempty" db:"-"` // list of children names
	ChildrenStepMap map[string]bool   `json:"children_step_map,omitempty" db:"-"`

	// action
	Schema json.RawMessage `json:"json_schema,omitempty" db:"-"`
	// hints about ETA latency, async, for retrier to define strategy
	// how often VS how many times
	RetryPattern   string        `json:"retry_pattern,omitempty" db:"-"` // seconds, minutes, hours
	ExecutionDelay time.Duration `json:"execution_delay,omitempty" db:"-"`

	Dependencies []string `json:"dependencies,omitempty" db:"-"`
	CustomStates []string `json:"custom_states,omitempty" db:"-"`

	// loop
	ForEach         string   `json:"foreach,omitempty" db:"-"` // "parent" step: expression for list of items
	ForEachStrategy string   `json:"foreach_strategy" db:"-"`
	Resources       []string `json:"resources" db:"-"` // resource limits to enforce

	// Encrypted
	Children   []interface{}          `json:"children,omitempty" db:"-"`
	Action     executor.Executor      `json:"action" db:"-"`
	Item       interface{}            `json:"item,omitempty" db:"-"` // "child" step: item value, issued from foreach
	Conditions []*condition.Condition `json:"conditions,omitempty" db:"-"`
	Output     interface{}            `json:"output,omitempty" db:"-"`
	Metadata   interface{}            `json:"metadata,omitempty" db:"-"`
	Error      string                 `json:"error,omitempty" db:"-"`
	PreHook    *executor.Executor     `json:"pre_hook,omitempty" db:"-"`
}

// PreInsert handles marshalling, compression and encryption of the Step's data before it's inserted in the DB
func (s *Step) PreInsert(e gorp.SqlExecutor) error {
	return s.encryptMarshalStepData()
}

// PostInsert clears the Step's encrypted data after it was inserted to reduce memory usage
func (s *Step) PostInsert(e gorp.SqlExecutor) error {
	s.EncryptedStepData = ""
	return nil
}

// PreInsert handles marshalling, compression and encryption of the Step's data before it's updated in the DB
func (s *Step) PreUpdate(e gorp.SqlExecutor) error {
	return s.encryptMarshalStepData()
}

// PostInsert clears the Step's encrypted data after it was inserted to reduce memory usage
func (s *Step) PostUpdate(e gorp.SqlExecutor) error {
	s.EncryptedStepData = ""
	return nil
}

// PostGet handles decryption, decompression and unmarshalling of the Step's data after it was retrieved from
// the database
func (s *Step) PostGet(e gorp.SqlExecutor) error {
	return s.decryptMarshalStepData()
}

// ValidAndNormalizeNewStep will validate that a given step doesn't have extragenous fields defined
// when coming from a task_template.
func (st *Step) ValidAndNormalizeNewStep() error {
	// check that we don't set restricted field from the template
	if st.State != "" {
		return errors.NewNotValid(nil, "step state must not be set")
	}

	if st.ChildrenSteps != nil {
		return errors.NewNotValid(nil, "step children_steps must not be set")
	}

	if st.ChildrenStepMap != nil {
		return errors.NewNotValid(nil, "step children_steps_map must not be set")
	}

	if st.Output != nil {
		return errors.NewNotValid(nil, "step output must not be set")
	}

	if st.Metadata != nil {
		return errors.NewNotValid(nil, "step metadatas must not be set")
	}

	if st.Tags != nil {
		return errors.NewNotValid(nil, "step tags must not be set")
	}

	if st.Children != nil {
		return errors.NewNotValid(nil, "step children must not be set")
	}

	if st.Error != "" {
		return errors.NewNotValid(nil, "step error must not be set")
	}

	if st.TryCount != 0 {
		return errors.NewNotValid(nil, "step try_count must not be set")
	}

	t := time.Time{}
	if st.LastRun != t {
		return errors.NewNotValid(nil, "step last_time must not be set")
	}

	if st.Item != nil {
		return errors.NewNotValid(nil, "step item must not be set")
	}

	return nil
}

// IsRunnable asserts that Step is in a runnable state
func (st *Step) IsRunnable() bool {
	return RunnableStates.Contains(st.State)
}

// IsRetriable asserts that Step is eligible for retry
func (st *Step) IsRetriable() bool {
	return RetriableStates.Contains(st.State)
}

// IsFinal asserts that Step is in a final step (not to be run again)
func (st *Step) IsFinal() bool {
	return (st.State != StateRunning && !st.IsRunnable())
}

// IsChild asserts that Step was spawned by a foreach step
func (st *Step) IsChild() bool {
	return st.Item != nil
}

func LoadSteps(dbp zesty.DBProvider, resolutionID int64) ([]*Step, error) {
	stepsQuery, params, err := sqlgenerator.PGsql.
		Select("*").
		From("step").
		Where(squirrel.Eq{"resolution_id": resolutionID}).
		ToSql()
	if err != nil {
		return nil, err
	}

	var steps []*Step
	if _, err := dbp.DB().Select(&steps, stepsQuery, params...); err != nil {
		return nil, pgjuju.Interpret(err)
	}

	return steps, nil
}

func LoadStepsAsMap(dbp zesty.DBProvider, resolutionID int64) (map[string]*Step, error) {
	steps, err := LoadSteps(dbp, resolutionID)
	if err != nil {
		return nil, err
	}

	stepMap := make(map[string]*Step)
	for _, s := range steps {
		stepMap[s.Name] = s
	}
	return stepMap, nil
}

func (s *Step) encryptMarshalStepData() error {
	c, err := compress.Get(s.CompressionAlg)
	if err != nil {
		return err
	}

	jsonStepData, err := json.Marshal(s.DBModel)
	if err != nil {
		return err
	}

	compressedStepData, err := c.Compress(jsonStepData)
	if err != nil {
		return err
	}

	res, err := models.EncryptionKey.Encrypt(compressedStepData)
	if err != nil {
		return err
	}

	s.EncryptedStepData = string(res)
	return nil
}

func (s *Step) decryptMarshalStepData() error {
	// Steps were found in the Resolution, we parse them as usual
	c, err := compress.Get(s.CompressionAlg)
	if err != nil {
		return err
	}

	dst := make([]byte, hex.DecodedLen(len(s.EncryptedStepData)))

	// if we can't hex Decode, we might be in the case of a Resolution row in database that was
	// created between the v1.21.1 and v1.21.3 that was bugged, and failed to hex Encode/Decode the
	// ciphered data. We need to keep backward compatibility for those, but this should not happen
	// often.
	// See https://github.com/ovh/utask/commit/bf23fbb10b62bb487ac4ea01b1e519f85480e58b and migration
	// from symmecrypt.Key.DecryptMarshal to symmecrypt.Key.Decrypt
	if _, err = hex.Decode(dst, []byte(s.EncryptedStepData)); err != nil {
		dst = []byte(s.EncryptedStepData)
	}

	compressedSteps, err := models.EncryptionKey.Decrypt(dst)
	if err != nil {
		return err
	}

	jsonSteps, err := c.Decompress(compressedSteps)
	if err != nil {
		return err
	}

	if err := utils.JSONnumberUnmarshal(bytes.NewReader(jsonSteps), &s.StepData); err != nil {
		return err
	}

	return nil
}

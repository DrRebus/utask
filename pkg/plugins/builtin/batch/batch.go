package pluginbatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	jujuErrors "github.com/juju/errors"
	"github.com/loopfz/gadgeto/zesty"
	"github.com/sirupsen/logrus"

	"github.com/ovh/utask"
	"github.com/ovh/utask/models/resolution"
	"github.com/ovh/utask/models/task"
	"github.com/ovh/utask/models/tasktemplate"
	"github.com/ovh/utask/pkg/auth"
	"github.com/ovh/utask/pkg/batch"
	"github.com/ovh/utask/pkg/batchutils"
	"github.com/ovh/utask/pkg/constants"
	"github.com/ovh/utask/pkg/metadata"
	"github.com/ovh/utask/pkg/plugins/taskplugin"
	"github.com/ovh/utask/pkg/templateimport"
	"github.com/ovh/utask/pkg/utils"
)

// The batch plugin spawns X new µTask tasks, given a template and inputs, and waits for them to be completed.
// Resolver usernames can be dynamically set for the task
var Plugin = taskplugin.New(
	"batch",
	"0.1",
	exec,
	taskplugin.WithConfig(validConfigBatch, BatchConfig{}),
	taskplugin.WithContextFunc(ctxBatch),
)

// BatchConfig is the necessary configuration to spawn a new task
type BatchConfig struct {
	TemplateName      string                   `json:"template_name" binding:"required"`
	CommonInput       map[string]interface{}   `json:"common_input"`
	JSONCommonInput   string                   `json:"json_common_input"`
	Inputs            []map[string]interface{} `json:"inputs"`
	JSONInputs        string                   `json:"json_inputs"`
	Comment           string                   `json:"comment"`
	WatcherUsernames  []string                 `json:"watcher_usernames"`
	WatcherGroups     []string                 `json:"watcher_groups"`
	Tags              map[string]string        `json:"tags"`
	ResolverUsernames string                   `json:"resolver_usernames"`
	ResolverGroups    string                   `json:"resolver_groups"`
	// How many tasks will run concurrently. 0 for infinity (default)
	SubBatchSize int `json:"sub_batch_size"`
	// Whether or not to gather child tasks' results. By default, no output is gathered to save performance.
	GatherOutputs bool `json:"gather_outputs"`
}

// utaskString is a string with doubly escaped quotes, so the string stays simply escaped after being processed
// as the plugin context (see ctxBatch).
type utaskString string

// BatchContext holds data about the parent task as well as the metadata of previous runs, if any.
type BatchContext struct {
	ParentTaskID      string `json:"parent_task_id"`
	RequesterUsername string `json:"requester_username"`
	RequesterGroups   string `json:"requester_groups"`
	// RawMetadata of the previous run. Metadata are used to communicate batch progress between runs. It's returned
	// "as is" in case something goes wrong in a subsequent run, to know what the batch's progress was when the
	// error occured.
	RawMetadata utaskString `json:"metadata"`
	// Unmarshalled version of the metadata
	metadata BatchMetadata
	StepName string `json:"step_name"`
	// RawOutput is the output of the previous run. It's used to aggregate previous results with new ones.
	RawOutput utaskString `json:"output"`
	output    TasksOutputs
}

// BatchMetadata holds batch-progress data, communicated between each run of the plugin.
type BatchMetadata struct {
	BatchID        string `json:"batch_id"`
	RemainingTasks int64  `json:"remaining_tasks"`
	TasksStarted   int64  `json:"tasks_started"`
	// Last task whose result was gathered. Useful to list the next tasks whose result should be gathered
	LastGatheredTask string `json:"last_gathered_task"`
}

// TasksOutputs aggregates the output of each task.
type TasksOutputs [][]byte

type BatchResult struct {
	Metadata  BatchMetadata
	Output    TasksOutputs
	StepError error
}

func ctxBatch(stepName string) interface{} {
	return &BatchContext{
		ParentTaskID:      "{{ .task.task_id }}",
		RequesterUsername: "{{.task.requester_username}}",
		RequesterGroups:   "{{ if .task.requester_groups }}{{ .task.requester_groups }}{{ end }}",
		RawMetadata: utaskString(fmt.Sprintf(
			"{{ if (index .step `%s` ) }}{{ if (index .step `%s` `metadata`) }}{{ index .step `%s` `metadata` }}{{ end }}{{ end }}",
			stepName,
			stepName,
			stepName,
		)),
		StepName: stepName,
		RawOutput: utaskString(fmt.Sprintf(
			"{{ if (index .step `%s` ) }}{{ if (index .step `%s` `output`) }}{{ index .step `%s` `output` }}{{ end }}{{ end }}",
			stepName,
			stepName,
			stepName,
		)),
	}
}

func validConfigBatch(config any) error {
	conf := config.(*BatchConfig)

	if err := utils.ValidateTags(conf.Tags); err != nil {
		return err
	}

	dbp, err := zesty.NewDBProvider(utask.DBName)
	if err != nil {
		return fmt.Errorf("can't retrieve connection to DB: %s", err)
	}

	_, err = tasktemplate.LoadFromName(dbp, conf.TemplateName)
	if err != nil {
		if !jujuErrors.IsNotFound(err) {
			return fmt.Errorf("can't load template from name: %s", err)
		}

		// searching into currently imported templates
		templates := templateimport.GetTemplates()
		for _, template := range templates {
			if template == conf.TemplateName {
				return nil
			}
		}

		return jujuErrors.NotFoundf("batch template %q", conf.TemplateName)
	}

	return nil
}

func exec(stepName string, config any, ictx any) (any, any, error) {
	conf := config.(*BatchConfig)
	batchCtx := ictx.(*BatchContext)
	if err := parseInputs(conf, batchCtx); err != nil {
		return batchCtx.RawOutput.Format(), batchCtx.RawMetadata.Format(), err
	}

	if conf.Tags == nil {
		conf.Tags = map[string]string{}
	}
	conf.Tags[constants.SubtaskTagParentTaskID] = batchCtx.ParentTaskID

	ctx := auth.WithIdentity(context.Background(), batchCtx.RequesterUsername)
	requesterGroups := strings.Split(batchCtx.RequesterGroups, utask.GroupsSeparator)
	ctx = auth.WithGroups(ctx, requesterGroups)

	dbp, err := zesty.NewDBProvider(utask.DBName)
	if err != nil {
		return batchCtx.RawOutput.Format(), batchCtx.RawMetadata.Format(), err
	}

	if err := dbp.Tx(); err != nil {
		return batchCtx.RawOutput.Format(), batchCtx.RawMetadata.Format(), err
	}

	var batchResult BatchResult
	if batchCtx.metadata.BatchID == "" {
		// The batch needs to be started
		batchResult, err = startBatch(ctx, dbp, conf, batchCtx)
		if err != nil {
			dbp.Rollback()
			return nil, nil, err
		}
	} else {
		// Batch already started, we either need to start new tasks or check whether they're all done
		batchResult, err = runBatch(ctx, conf, batchCtx, dbp)
		if err != nil {
			dbp.Rollback()
			return batchCtx.RawOutput.Format(), batchCtx.RawMetadata.Format(), err
		}
	}

	if err := dbp.Commit(); err != nil {
		dbp.Rollback()
		return batchCtx.RawOutput.Format(), batchCtx.RawMetadata.Format(), err
	}

	return batchResult.Format()
}

// startBatch creates a batch a tasks as described in the given batchArgs.
func startBatch(
	ctx context.Context,
	dbp zesty.DBProvider,
	conf *BatchConfig,
	batchCtx *BatchContext,
) (BatchResult, error) {
	b, err := task.CreateBatch(dbp)
	if err != nil {
		return BatchResult{}, err
	}

	taskIDs, err := populateBatch(ctx, b, dbp, conf, batchCtx)
	if err != nil {
		return BatchResult{}, err
	}

	stepError := jujuErrors.NewNotAssigned(fmt.Errorf("tasks from batch %q will start shortly", metadata.BatchID), "")
	return BatchResult{
		Metadata: BatchMetadata{
			BatchID:        b.PublicID,
			RemainingTasks: int64(len(conf.Inputs)),
			TasksStarted:   int64(len(taskIDs)),
		},
		// A step returning a NotAssigned error is set to WAITING by the engine
		StepError: stepError,
	}, nil
}

// populateBatch spawns new tasks in the batch and returns their public identifier.
func populateBatch(
	ctx context.Context,
	b *task.Batch,
	dbp zesty.DBProvider,
	conf *BatchConfig,
	batchCtx *BatchContext,
) ([]string, error) {
	tasksStarted := batchCtx.metadata.TasksStarted
	running, err := batchutils.RunningTasks(dbp, b.ID)
	if err != nil {
		return []string{}, err
	}

	// Computing how many tasks to start
	remaining := int64(len(conf.Inputs)) - tasksStarted
	toStart := int64(conf.SubBatchSize) - running // How many tasks can be started
	if remaining < toStart {
		toStart = remaining // There's less tasks to start remaining than the amount of available running slots
	}

	args := batch.TaskArgs{
		TemplateName:     conf.TemplateName,
		CommonInput:      conf.CommonInput,
		Inputs:           conf.Inputs[tasksStarted : tasksStarted+toStart],
		Comment:          conf.Comment,
		WatcherGroups:    conf.WatcherGroups,
		WatcherUsernames: conf.WatcherUsernames,
		Tags:             conf.Tags,
	}

	taskIDs, err := batch.Populate(ctx, b, dbp, args)
	if err != nil {
		return []string{}, err
	}

	return taskIDs, nil
}

// runBatch runs batch, spawning new tasks if needed and checking whether they're all done.
func runBatch(
	ctx context.Context,
	conf *BatchConfig,
	batchCtx *BatchContext,
	dbp zesty.DBProvider,
) (BatchResult, error) {
	batchResult := BatchResult{
		Metadata: batchCtx.metadata,
		Output:   batchCtx.output,
	}

	b, err := task.LoadBatchFromPublicID(dbp, batchCtx.metadata.BatchID)
	if err != nil {
		if jujuErrors.IsNotFound(err) {
			// The batch has been collected (deleted in DB) because no remaining task referenced it. There's
			// nothing more to do.
			return batchResult, nil
		}
		return batchResult, err
	}

	// A step returning a NotAssigned error is set to WAITING by the engine
	stepErrorWaiting := jujuErrors.NewNotAssigned(fmt.Errorf("batch %q is currently RUNNING", b.PublicID), "")

	running, err := batchutils.RunningTasks(dbp, b.ID)
	if err != nil {
		return batchResult, err
	}

	if running != 0 {
		// There still are tasks running, we shouldn't be running the batch again just yet. This can happen if the
		// resolution was run manually.
		batchResult.StepError = stepErrorWaiting
		return batchResult, nil
	}
	// else, all started tasks completed, we can spawn new tasks if needed and gather the outputs of tasks DONE.

	if batchCtx.metadata.TasksStarted < int64(len(conf.Inputs)) {
		// New tasks still need to be added to the batch
		taskIDs, err := populateBatch(ctx, b, dbp, conf, batchCtx)
		if err != nil {
			return batchResult, err
		}

		started := int64(len(taskIDs))
		batchResult.Metadata.TasksStarted += started
		batchResult.Metadata.RemainingTasks -= started // Starting X tasks means that X tasks became DONE
		batchResult.StepError = stepErrorWaiting
	} else {
		// The batch is done.
		// Increasing the resolution's maximum amount of retries to compensate for the amount of runs consumed
		// by child tasks waking up the parent when they're done.
		err := increaseRunMax(dbp, batchCtx.ParentTaskID, batchCtx.StepName)
		if err != nil {
			return batchResult, err
		}
		batchResult.Metadata.RemainingTasks = running
	}

	if conf.GatherOutputs {
		outputs, lastGatheredTask, err := gatherOutputs(dbp, b.ID, batchCtx.metadata.LastGatheredTask)
		if err != nil {
			return batchResult, err
		}
		batchResult.Output = append(batchResult.Output, outputs...)
		batchResult.Metadata.LastGatheredTask = lastGatheredTask
	}

	return batchResult, nil
}

// increaseRunMax increases the maximum amount of runs of the resolution matching the given parentTaskID by the run
// count of the given batchStepName.
// Since child tasks wake their parent up when they're done, the resolution's RunCount gets incremented everytime. We
// compensate this by increasing the RunMax property once the batch is done.
func increaseRunMax(dbp zesty.DBProvider, parentTaskID string, batchStepName string) error {
	t, err := task.LoadFromPublicID(dbp, parentTaskID)
	if err != nil {
		return err
	}
	res, err := resolution.LoadLockedFromPublicID(dbp, *t.Resolution)
	if err != nil {
		return err
	}
	step, ok := res.Steps[batchStepName]
	if !ok {
		return fmt.Errorf("step '%s' not found in resolution", batchStepName)
	}

	res.ExtendRunMax(step.TryCount)
	return res.Update(dbp)
}

// parseInputs parses the step's inputs as well as metadata from the previous run (if it exists).
func parseInputs(conf *BatchConfig, batchCtx *BatchContext) error {
	if batchCtx.RawMetadata != "" {
		// Metadata from a previous run is available
		if err := json.Unmarshal([]byte(batchCtx.RawMetadata), &batchCtx.metadata); err != nil {
			return jujuErrors.NewBadRequest(err, "metadata unmarshalling failure")
		}
	}

	if batchCtx.RawOutput != "" {
		// Output from a previous run is available
		if err := json.Unmarshal([]byte(batchCtx.RawOutput), &batchCtx.output); err != nil {
			return jujuErrors.NewBadRequest(err, "output unmarshalling failure")
		}
	}

	if conf.JSONCommonInput != "" {
		if err := json.Unmarshal([]byte(conf.JSONCommonInput), &conf.CommonInput); err != nil {
			return jujuErrors.NewBadRequest(err, "JSON common input unmarshalling failure")
		}
	}

	if conf.JSONInputs != "" {
		if err := json.Unmarshal([]byte(conf.JSONInputs), &conf.Inputs); err != nil {
			return jujuErrors.NewBadRequest(err, "JSON inputs unmarshalling failure")
		}
	}
	return nil
}

// gatherOutputs gathers the output of all DONE tasks in the given batch. To prevent reading the same results twice,
// only tasks created after the one whose ID is lastTaskPublicID are considered.
func gatherOutputs(dbp zesty.DBProvider, batchID int64, lastTaskPublicID string) (TasksOutputs, string, error) {
	outputs, err := batchutils.GetRawTasksOutputs(dbp, batchID, lastTaskPublicID)
	if err != nil {
		return nil, "", err
	}

	tasksOutputs := make(TasksOutputs, 0, len(outputs))
	for _, output := range outputs {
		tasksOutputs = append(tasksOutputs, output.Output)
	}
	return tasksOutputs, outputs[len(outputs)-1].PublicID, nil
}

// Format formats the utaskString to make sure it's parsable by subsequent runs of the plugin (i.e.: escaping
// double quotes).
func (rm utaskString) Format() string {
	return strings.ReplaceAll(string(rm), `"`, `\"`)
}

// FormatFinal formats the BatchOutput so that it can be used in other steps.
func (bo TasksOutputs) FormatFinal() (any, error) {
	cleanOutput := make([]any, 0, len(bo))
	for _, out := range bo {
		var result any
		err := utils.JSONnumberUnmarshal(bytes.NewReader(out), &result)
		if err != nil {
			return nil, err
		}
		cleanOutput = append(cleanOutput, result)
	}
	return cleanOutput, nil
}

// Format formats the batch's result (output and metadata) according to its progress. When a batch needs to
// communicate data to itself between runs, metadata and output need to be formatted in a special way. This is due
// to how plugins' contexts work for complex data types (e.g.: structs, arrays, etc).
func (br BatchResult) Format() (output any, metadata any, err error) {
	marshalledMetadata, err := json.Marshal(br.Metadata)
	if err != nil {
		logrus.WithError(err).Error("Couldn't marshal batch metadata")
		return nil, nil, err
	}
	metadata = utaskString(marshalledMetadata).Format()

	if br.Metadata.RemainingTasks != 0 {
		// Partial output formatting. More tasks still need their output to be gathered, so we format current outputs
		// in a way that is easily parsable by the plugin in following runs.
		marshalledOutput, err := json.Marshal(br.Output)
		if err != nil {
			logrus.WithError(err).Error("Couldn't marshal batch output")
			return nil, nil, err
		}
		output = utaskString(marshalledOutput).Format()
	} else {
		// No more tasks' outputs to gather, we can now format the output in a "clean" way, usable by other steps.
		output, err = br.Output.FormatFinal()
		if err != nil {
			return nil, nil, err
		}
	}

	return output, metadata, br.StepError
}

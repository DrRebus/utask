package batchutils

import (
	"github.com/Masterminds/squirrel"
	"github.com/loopfz/gadgeto/zesty"

	"github.com/ovh/utask/db/sqlgenerator"
	"github.com/ovh/utask/models"
	"github.com/ovh/utask/models/task"
)

// States in which the task won't ever be run again
var FinalStates = []string{task.StateDone, task.StateCancelled, task.StateWontfix}

// RunningTasks returns the amount of running tasks sharing the same given batchId.
func RunningTasks(dbp zesty.DBProvider, batchId int64) (int64, error) {
	query, params, err := sqlgenerator.PGsql.
		Select("count (*)").
		From("task t").
		Join("batch b on b.id = t.id_batch").
		Where(squirrel.Eq{"b.id": batchId}).
		Where(squirrel.NotEq{"t.state": FinalStates}).
		ToSql()
	if err != nil {
		return -1, err
	}

	return dbp.DB().SelectInt(query, params...)
}

// pageSize is the maximum amount of tasks to list at once when gathering results
const pageSize = 100

type TaskOutput struct {
	PublicID string // Public ID of the task
	Output   []byte
}

// GetRawTasksOutputs gathers the output of all DONE tasks in the given batch. Only tasks created after the one with
// ID lastTaskPublicID are gathered.
func GetRawTasksOutputs(dbp zesty.DBProvider, batchID int64, lastTaskPublicID string) ([]TaskOutput, error) {
	var lastTaskID int64
	outputs := []TaskOutput{}

	if lastTaskPublicID != "" {
		// A public ID was given, we'll only retrieve results of tasks created after this one
		t, err := task.LoadFromPublicID(dbp, lastTaskPublicID)
		if err != nil {
			return outputs, err
		}
		lastTaskID = t.ID
	}

	// Base query to get tasks outputs. We'll set the task's ID (used as a selector) as we go.
	baseQuery := sqlgenerator.PGsql.
		Select("t.id, t.public_id, t.encrypted_result").
		From("task t").
		Where(squirrel.Eq{"t.id_batch": batchID}).
		Where(squirrel.Eq{"t.state": task.StateDone}).
		Limit(uint64(pageSize))

	for {
		tasks := []task.Task{}
		query, params, err := baseQuery.
			Where(squirrel.Gt{"t.id": lastTaskID}).
			ToSql()
		if err != nil {
			return outputs, err
		}

		_, err = dbp.DB().Select(&tasks, query, params...)
		if err != nil {
			return outputs, err
		}

		partialOutputs := make([]TaskOutput, 0, len(tasks)) // Preallocation for faster appends
		for _, t := range tasks {
			resBytes, err := models.EncryptionKey.Decrypt(t.EncryptedResult, []byte(t.PublicID))
			if err != nil {
				return outputs, err
			}

			partialOutputs = append(partialOutputs, TaskOutput{PublicID: t.PublicID, Output: resBytes})
			lastTaskID = t.ID
		}
		outputs = append(outputs, partialOutputs...)

		if len(tasks) < pageSize {
			// No more tasks to retrieve
			break
		}
	}

	return outputs, nil
}

# `batch` Plugin

This plugin creates a batch of tasks based on the same template and waits for it to complete. It acts like the `subtask` combined with a `foreach`, but doesn't modify the resolution by adding new steps dynamically. As it makes less calls to the underlying database, this plugin is suited for large batches of task, where the `subtask` + foreach combination would usually struggle, escpecially by bloating the database.

Tasks belonging to the same batch share a common `BatchID` as well as tag holding their parent's ID.
Once all tasks are done, their output can be found in the step's output field, if `gather_outputs` was specified (see Configuration bellow).


## Configuration

| Fields               | Description                                                                                                       |
|----------------------|-------------------------------------------------------------------------------------------------------------------|
| `template_name`      | the name of a task template, as accepted through µTask's  API                                                     |
| `inputs`             | a list of map of named values, as accepted on µTask's API. One task will be created by map in the list            |
| `json_inputs`        | same as `inputs`, but as a JSON string. If specified, it overrides `inputs`                                       |
| `common_input`       | a map of named values, as accepted on µTask's API, given to all task in the batch by combining it with each input |
| `json_common_input`  | same a `common_input` but as a JSON string. If specified, it overrides `common_input`                             |
| `tags`               | a map of named strings added as tags when creating child tasks                                                    |
| `sub_batch_size`     | how many tasks to create and run at once. 0 for infinity (i.e.: all tasks are created at once and waited for).(default). Higher values reduce the amount calls made to the database, but increase sensitivity to database unavailability (if a task creation fails, the whole sub batch must be created again) |
| `gather_outputs`     | whether or not to gather and aggregate outputs of child tasks. (default: false)                                   |
| `comment`            | a string set as `comment` when creating child tasks                                                               |
| `resolver_usernames` | a string containing a JSON array of additional resolver users for child tasks                                     |
| `resolver_groups`    | a string containing a JSON array of additional resolver groups for child tasks                                    |
| `watcher_usernames`  | a string containing a JSON array of additional watcher users for child tasks                                      |
| `watcher_groups`     | a string containing a JSON array of additional watcher groups for child tasks                                     |

## Example

An action of type `batch` requires the following kind of configuration:

```yaml
action:
  type: batch
  configuration:
    # [Required]
    # A template that must already be registered on this instance of µTask
    template: some-task-template
    # Valid inputs, as defined by the referred template, here requiring 3 inputs: foo, otherFoo and fooCommon
    inputs:
        - foo: bar-1
          otherFoo: otherBar-1
        - foo: bar-2
          otherFoo: otherBar-1
        - foo: bar-3
          otherFoo: otherBar-3
    # [Optional]
    common_input:
        fooCommon: barCommon
    # Some tags added to all child tasks
    tags:
        fooTag: value-of-foo-tag
        barTag: value-of-bar-tag
    # The amount of tasks to run at once
    sub_batch_size: 2
    # Gather outputs of child tasks
    gather_outputs: true
    # A list of users which are authorized to resolve this specific task
    resolver_usernames: '["authorizedUser"]'
    resolver_groups: '["authorizedGroup"]'
    watcher_usernames: '["authorizedUser"]'
    watcher_groups: '["authorizedGroup"]'
```

## Requirements

None.

## Return

### Output

The list of all tasks' outputs, in the same order as the given `inputs`

### Metadata

| Name                 | Description                                   |
|----------------------|-----------------------------------------------|
| `batch_id`           | The public identifier of the batch            |
| `remaining_tasks`    | How many tasks still need to complete         |
| `tasks_started`      | How many tasks were started so far            |
| `last_gathered_task` | ID of the last task whose output was gathered |

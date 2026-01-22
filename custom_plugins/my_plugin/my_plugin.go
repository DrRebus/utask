package myplugin

import (
	"fmt"

	"github.com/ovh/utask/pkg/plugins/taskplugin"
)

// The batch plugin spawns X new µTask tasks, given a template and inputs, and waits for them to be completed.
// Resolver usernames can be dynamically set for the task
var Plugin = taskplugin.New(
	"my-plugin",
	"1.0",
	exec,
	taskplugin.WithConfig(validConfig, MyPluginConf{}),
	taskplugin.WithContextFunc(ctxPlugin),
)

type MyPluginConf struct {
	StringParam string `json:"string_param" binding:"required"`
}

type PluginContext struct {
	StepName          string `json:"step_name"`
	RequesterUsername string `json:"requester_username"`
	RequesterGroups   string `json:"requester_groups"`
}

func ctxPlugin(stepName string) interface{} {
	return &PluginContext{
		RequesterUsername: "{{.task.requester_username}}",
		RequesterGroups:   "{{ if .task.requester_groups }}{{ .task.requester_groups }}{{ end }}",
		StepName:          stepName,
	}
}

func validConfig(config any) error {
	return nil
}

func exec(stepName string, config any, ictx any) (any, any, error) {
	conf := config.(*MyPluginConf)
	// plgCtx := ictx.(*PluginContext)

	fmt.Println("WE IN DA PLUGIN!")

	return "SHEE HOO " + conf.StringParam, "HOORAY " + conf.StringParam, nil
}

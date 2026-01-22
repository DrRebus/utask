package main

import (
	"os"

	"github.com/sirupsen/logrus"

	myPlg "github.com/ovh/utask/custom_plugins/my_plugin"
	"github.com/ovh/utask/engine/step"
	"github.com/ovh/utask/launcher"
)

func main() {
	launcher.InitUtask()
	if err := initCustomPlugins(); err != nil {
		logrus.WithError(err).Fatalf("Couldn't init custom plugins")
	}
	if err := launcher.RunUtask(); err != nil {
		logrus.WithError(err).Fatalf("Couldn't run utask")
	}

	os.Exit(0)
}

func initCustomPlugins() error {
	err := step.RegisterRunner(myPlg.Plugin.PluginName(), myPlg.Plugin)
	if err != nil {
		return err
	}
	return nil
}

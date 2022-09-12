package main

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("command/reference-provider")

var daemonFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "log-level",
		Usage:    "Set the log level",
		EnvVars:  []string{"GOLOG_LOG_LEVEL"},
		Value:    "info",
		Required: false,
	},
}

var DaemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Starts a reference provider",
	Flags:  daemonFlags,
	Action: daemonCommand,
}

func daemonCommand(cctx *cli.Context) error {
	err := logging.SetLogLevel("*", cctx.String("log-level"))
	if err != nil {
		return err
	}

	return nil
}

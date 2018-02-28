package qhandler_dgraph

import (
	"fmt"
	"log"
	"strings"
	"google.golang.org/grpc"
	"github.com/zpatrick/go-config"
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos/api"

	"github.com/qframe/types/plugin"
	"github.com/qframe/types/constants"
	"github.com/qframe/types/qchannel"
	"github.com/qframe/types/messages"
	"github.com/qframe/cache-inventory"
	"github.com/qframe/types/docker-events"
)

const (
	version   = "0.0.1"
	pluginTyp = qtypes_constants.HANDLER
	pluginPkg = "dgraph"
)

// Plugin holds a buffer and the initial information from the server
type Plugin struct {
	*qtypes_plugin.Plugin
	cli *client.Dgraph
}

// New returns an initial instance
func New(qChan qtypes_qchannel.QChan, cfg *config.Config, name string) (Plugin, error) {
	p := Plugin{
		Plugin: qtypes_plugin.NewNamedPlugin(qChan, cfg, pluginTyp, pluginPkg, name, version),
	}
	p.Version = version
	p.Name = name
	return p, nil
}


func (p *Plugin) Connect() {

	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	d, err := grpc.Dial(p.CfgStringOr("dgraph-server","task.dgraph-server:9081"), grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	p.cli = client.NewDgraphClient(api.NewDgraphClient(d))
}


// Run pushes the logs to elasticsearch
func (p *Plugin) Run() {
	p.Log("notice", fmt.Sprintf("Start %s handler: %sv%s", pluginPkg, p.Name, version))
	bg := p.QChan.Data.Join()
	p.Connect()
	for {
		select {
		case val := <-bg.Read:
			switch val.(type) {
			// TODO: That has to be an interface!
			case qtypes_messages.Message:
				qm := val.(qtypes_messages.Message)
				if qm.StopProcessing(p.Plugin, false) {
					continue
				}
				p.Log("info" , fmt.Sprintf("%-15s : %s", strings.Join(qm.SourcePath, "->"), qm.Msg))
			case qcache_inventory.ContainerRequest:
				continue
			case qtypes_docker_events.ContainerEvent:
				ce := val.(qtypes_docker_events.ContainerEvent)
				p.handleContainerEvent(ce)
	        //default:
			//	p.Log("info", fmt.Sprintf("Dunno type '%s': %v", reflect.TypeOf(val), val))
			}
		}
	}
}

func (p *Plugin) handleContainerEvent(ce qtypes_docker_events.ContainerEvent) {
	switch ce.Event.Action {
	case "exec_create", "exec_start", "exec_die":
		return
	default:
		p.Log("info", fmt.Sprintf("%s -> %-20s | %s", ce.Event.Type, ce.Event.Action, ce.Event.Actor.Attributes["name"]))
	}

}
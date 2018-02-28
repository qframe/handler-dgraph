package qhandler_dgraph

import (
	"context"
	"encoding/json"
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
	"github.com/docker/docker/api/types/events"
)

const (
	version   = "0.0.1"
	pluginTyp = qtypes_constants.HANDLER
	pluginPkg = "dgraph"
)

var (
	ctx = context.Background()
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
	dserver := p.CfgStringOr("dgraph-server","task.dgraph-server:9081")
	d, err := grpc.Dial(dserver, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	p.cli = client.NewDgraphClient(api.NewDgraphClient(d))
	p.Log("info", fmt.Sprintf("Connected to server '%s'", dserver))
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
	case "start":
		if ce.Event.Actor.ID != "" {
			p.createCntObj(ce.Event)
		}
	default:
		p.Log("info", fmt.Sprintf("%s -> %-20s | %s | %s", ce.Event.Type, ce.Event.Action, ce.Event.Actor.ID[:12], ce.Event.Actor.Attributes["name"]))
	}

}

func (p *Plugin) createCntObj(e events.Message) (err error){
	// Install a schema into dgraph.

	// Containers have a `name`, `id` and a `image_name`.
	schema := `
			id: string @index(term) .
			name: string @index(term) .
			image: string .
	`

	p.indexDb(schema)
	// Remove
	err = p.cli.Alter(ctx, &api.Operation{DropAll: true})
	if err != nil {
		p.Log("error", err.Error())
		return err
	}
	cnt := Container{
		ID: e.Actor.ID[:12],
		Name: e.Actor.Attributes["name"],
		Image: e.Actor.Attributes["image"],
	}
	// Insert container
	mu := &api.Mutation{
		CommitNow: true,
	}
	pb, err := json.Marshal(cnt)
	if err != nil {
		p.Log("error", err.Error())
		return err
	}

	mu.SetJson = pb
	_, err = p.cli.NewTxn().Mutate(ctx, mu)
	if err != nil {
		p.Log("error", err.Error())
		return err
    }
	p.indexDb(schema)
	return

}

func (p *Plugin) indexDb(schema string) (err error) {
	err = p.cli.Alter(ctx, &api.Operation{Schema: schema})
	if err != nil {
		p.Log("error", err.Error())
	}
	return err
}
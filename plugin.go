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
	"reflect"
)

const (
	version   = "0.0.2"
	pluginTyp = qtypes_constants.HANDLER
	pluginPkg = "dgraph"
)

var (
	ctx = context.Background()
	// Containers have a `name`, `id` and a `image_name`.
	schema = map[string]string{
		"container": `
			id: string @index(term) .
			name: string @index(term) .
			image: string  @index(term) .
		`,
		"network": `
			id: string @index(term) .
			name: string @index(term) .
			driver: string  @index(term) .
			scope: string  @index(term) .
		`,
		"node": `
			id: string @index(term) .
			name: string @index(term) .
			status: string  @index(term) .
			availability: string  @index(term) .
			manager-status: string  @index(term) .
		`,
	}
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
	p.cleanDgraph()
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
			case qtypes_docker_events.NetworkEvent:
				ne := val.(qtypes_docker_events.NetworkEvent)
				p.handleNetworkEvent(ne)
			case qtypes_docker_events.DockerEvent:
				de := val.(qtypes_docker_events.DockerEvent)
				//TODO: Extract engine info
				p.Log("info", fmt.Sprintf("Got word: connected to '%s': %v", de.Engine.Name, de.Engine.ServerVersion))
				continue
			default:
				p.Log("info", fmt.Sprintf("Dunno type '%s': %v", reflect.TypeOf(val), val))
			}
		}
	}
}

func (p *Plugin) handleNetworkEvent(ne qtypes_docker_events.NetworkEvent) {
	switch ne.Event.Action {
	case "create":
		p.createNetwork(ne)
	case "update":
		p.Log("info", fmt.Sprintf("Network '%s' -> %v | Name: %s", ne.Network.ID, ne.Event.Action, ne.Network.Name))
	case "remove":
		p.removeObj(ne.Event)
	}

}

func (p *Plugin) handleContainerEvent(ce qtypes_docker_events.ContainerEvent) {
	switch ce.Event.Action {
	case "exec_create", "exec_start", "exec_die":
		return
	case "running":
		// Already running container
		//TODO: check whether ContainerID is already in graph!
		return
	case "start":
		p.createObj(ce.Event)
	default:
		p.Log("info", fmt.Sprintf("%s -> %-20s | %s | %s", ce.Event.Type, ce.Event.Action, ce.Event.Actor.ID[:12], ce.Event.Actor.Attributes["name"]))
	}

}

func (p *Plugin) cleanDgraph() (err error) {
	// Remove
	err = p.cli.Alter(ctx, &api.Operation{DropAll: true})
	if err != nil {
		p.Log("error", err.Error())
	}
	return err
}

func (p *Plugin) createContainer(cnt Container) (err error) {
	// Insert container
	mu := &api.Mutation{
		CommitNow: true,
	}
	pb, err := json.Marshal(cnt)
	if err != nil {
		return err
	}

	mu.SetJson = pb
	_, err = p.cli.NewTxn().Mutate(ctx, mu)
	if err != nil {
		return err
	}
	return
}

func (p *Plugin) createNetwork(ne qtypes_docker_events.NetworkEvent) (err error) {
	mu := &api.Mutation{
		CommitNow: true,
	}
	net := Network{
		ID: ne.Network.ID,
		Name: ne.Network.Name,
		Scope: ne.Network.Scope,
		Driver: ne.Network.Driver,
	}
	pb, err := json.Marshal(net)
	if err != nil {
		return err
	}

	mu.SetJson = pb
	_, err = p.cli.NewTxn().Mutate(ctx, mu)
	if err != nil {
		return err
	}
	p.indexDb("network")
	return
}

func (p *Plugin) removeObj(e events.Message) (err error) {
	p.Log("info", fmt.Sprintf("%s '%s' -> %v", e.Type, e.Actor.ID, e.Action))
	return
}

func (p *Plugin) createObj(e events.Message) (err error) {
	// Install a schema into dgraph.

	p.indexDb(e.Type)
	switch e.Type {

	case "container":
		cnt := Container{
			ID: e.Actor.ID[:12],
			Name: e.Actor.Attributes["name"],
			Image: e.Actor.Attributes["image"],
		}
		err = p.createContainer(cnt)
		if err != nil {
			return err
		}
	}

	p.indexDb("container")
	return
}


func (p *Plugin) indexDb(typ string) (err error) {
	err = p.cli.Alter(ctx, &api.Operation{Schema: schema[typ]})
	if err != nil {
		p.Log("error", err.Error())
	}
	return err
}
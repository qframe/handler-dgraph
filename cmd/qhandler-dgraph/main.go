package main

import (
	"log"
	"github.com/zpatrick/go-config"
	"github.com/qframe/collector-docker-events"
	"github.com/qframe/types/qchannel"
	"github.com/qframe/handler-dgraph"
)

func main() {
	qChan := qtypes_qchannel.NewQChan()
	qChan.Broadcast()
	cfgMap := map[string]string{
		"log.level": "info",
		"handler.dgraph.dgraph-server": "tasks.dgraph-server:9081",
	}
	cfg := config.NewConfig([]config.Provider{config.NewStatic(cfgMap)})
	// Create Health Cache\
	p, err := qhandler_dgraph.New(qChan, cfg, "dgraph")
	if err != nil {
		log.Fatalf("[EE] Failed to create dgraph: %v", err)
	}
	go p.Run()
	// Create docker events collector
	pde, err := qcollector_docker_events.New(qChan, cfg, "events")
	if err != nil {
		log.Fatalf("[EE] Failed to create docker-events: %v", err)
	}
	go pde.Run()
	bg := p.QChan.Data.Join()
	for {
		val := <- bg.Read
		switch val.(type) {
		default:
			continue
		}
	}
}


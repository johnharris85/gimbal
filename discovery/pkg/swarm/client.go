// Copyright © 2018 Heptio
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package swarm

import (
	"context"
	"net/http"

	"github.com/docker/cli/opts"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/client"
)

// SwarmClient is a SwarmClient
type SwarmClient struct {
	cli *client.Client
}

// NewSwarmClient returns new Docker Swarm client
func NewSwarmClient(apiVersion, daemonAddress string, httpClient *http.Client) (*SwarmClient, error) {
	c, err := client.NewClientWithOpts(
		client.WithVersion(apiVersion),
		client.WithHost(daemonAddress),
		client.WithHTTPClient(httpClient),
		client.WithScheme("https"),
	)
	if err != nil {
		return nil, err
	}
	return &SwarmClient{c}, nil
}

// ListSwarmServices returns a list of Swarm services in the cluster
func (cli *SwarmClient) ListSwarmServices() ([]swarm.Service, error) {
	services, err := cli.cli.ServiceList(context.Background(), types.ServiceListOptions{})
	if err != nil {
		return nil, err
	}
	var swarmServices []swarm.Service
	for _, service := range services {
		var hasIngressPort bool
		for _, portCfg := range service.Endpoint.Ports {
			if portCfg.PublishMode == "ingress" {
				hasIngressPort = true
			}
		}
		if hasIngressPort == true {
			swarmServices = append(swarmServices, service)
		}
	}
	return swarmServices, nil
}

// ListSwarmServiceNodes returns a list of nodes that the service is running on in the cluster
func (cli *SwarmClient) ListSwarmServiceNodes(service swarm.Service) (map[string]bool, error) {
	nodeIPSet := make(map[string]bool)

	filter := opts.NewFilterOpt()
	f := filter.Value()
	f.Add("service", service.ID)

	tasks, err := cli.cli.TaskList(context.Background(), types.TaskListOptions{Filters: f})
	if err != nil {
		return nil, err
	}
	for _, task := range tasks {
		if task.Status.State == "running" {
			nodeDetails, _, err := cli.cli.NodeInspectWithRaw(context.Background(), task.NodeID)
			if err != nil {
				return nil, err
			}
			nodeIPSet[nodeDetails.Status.Addr] = true
		}
	}
	return nodeIPSet, nil
}

// ListSwarmServicePorts returns a list of ports that the service is running on in the cluster
func (cli *SwarmClient) ListSwarmServicePorts(service swarm.Service) (map[uint32]bool, error) {
	portSet := make(map[uint32]bool)
	for _, portCfg := range service.Endpoint.Ports {
		if portCfg.PublishMode == "ingress" {
			portSet[portCfg.PublishedPort] = true
		}
	}
	return portSet, nil
}

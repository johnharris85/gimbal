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
	"log"
	"regexp"
	"strconv"
	"strings"

	dockerswarm "github.com/docker/docker/api/types/swarm"
	"github.com/heptio/gimbal/discovery/pkg/translator"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// returns a kubernetes service for each Swarm service
func kubeServices(backendName string, swarmLister SwarmServiceLister) []v1.Service {
	var svcs []v1.Service
	swarmServices, err := swarmLister.ListSwarmServices()
	if err != nil {
		log.Fatal(err)
	}
	for _, swarmService := range swarmServices {
		svc := v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: backendName,
				Name:      translator.BuildDiscoveredName(backendName, serviceName(swarmService)),
				Labels:    translator.AddGimbalLabels(backendName, serviceName(swarmService), swarmServiceLabels(swarmService)),
			},
			Spec: v1.ServiceSpec{
				Type:      v1.ServiceTypeClusterIP,
				ClusterIP: "None",
			},
		}
		for _, pCfg := range swarmService.Endpoint.Ports {
			svc.Spec.Ports = append(svc.Spec.Ports, servicePort(&pCfg))
		}
		svcs = append(svcs, svc)
	}
	return svcs
}

// returns a kubernetes endpoints resource for each load balancer in the slice
func kubeEndpoints(backendName string, swarmLister SwarmServiceLister) []Endpoints {
	endpoints := []Endpoints{}
	swarmServices, err := swarmLister.ListSwarmServices()
	if err != nil {
		log.Fatal(err)
	}
	for _, swarmService := range swarmServices {
		ep := v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: backendName,
				Name:      translator.BuildDiscoveredName(backendName, serviceName(swarmService)),
				Labels:    translator.AddGimbalLabels(backendName, serviceName(swarmService), swarmServiceLabels(swarmService)),
			},
		}

		// compute endpoint susbsets for each listener
		subset := v1.EndpointSubset{}

		// We want to group all members that are listening on the same port
		// into a single EndpointSubset. We achieve this by using a map of
		// subsets, keyed by the listening port.
		nodes, err := swarmLister.ListSwarmServiceNodes(swarmService)
		if err != nil {
			log.Fatal("Error")
		}
		for node := range nodes {
			subset.Addresses = append(subset.Addresses, v1.EndpointAddress{IP: node}) // TODO: can address be something other than an IP address?
		}

		ports, err := swarmLister.ListSwarmServicePorts(swarmService)
		if err != nil {
			log.Fatal("Error")
		}
		for port := range ports {
			subset.Ports = append(subset.Ports, v1.EndpointPort{Name: portName(&port), Port: int32(port), Protocol: v1.ProtocolTCP})
		}

		ep.Subsets = append(ep.Subsets, subset)
		endpoints = append(endpoints, Endpoints{endpoints: ep, upstreamName: serviceNameOriginal(swarmService)})
	}

	return endpoints
}

func swarmServiceLabels(swarmService dockerswarm.Service) map[string]string {
	// Sanitize the load balancer name according to the kubernetes label value
	// requirements: "Valid label values must be 63 characters or less and must
	// be empty or begin and end with an alphanumeric character ([a-z0-9A-Z])
	// with dashes (-), underscores (_), dots (.), and alphanumerics between."
	name := swarmService.Spec.Name
	if name != "" {
		// 1. replace unallowed chars with a dash
		reg := regexp.MustCompile("[^a-zA-Z0-9\\-._]")
		name = reg.ReplaceAllString(name, "-")

		// 2. prepend/append a special marker if first/last char is not an alphanum
		if !isalphanum(name[0]) {
			name = "swarmService" + name
		}
		if !isalphanum(name[len(name)-1]) {
			name = name + "swarmService"
		}
		// 3. shorten if necessary
		name = translator.ShortenKubernetesLabelValue(name)
	}
	return map[string]string{
		"gimbal.heptio.com/swarm-service-id":   swarmService.ID,
		"gimbal.heptio.com/swarm-service-name": name,
	}
}

func isalphanum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// use the load balancer ID as the service name
// context: heptio/gimbal #216
func serviceName(swarmService dockerswarm.Service) string {
	return strings.ToLower(swarmService.ID)
}

// get the lb Name or ID if name is empty
func serviceNameOriginal(swarmService dockerswarm.Service) string {
	lbName := swarmService.Spec.Name
	if lbName == "" {
		lbName = swarmService.ID
	}
	return strings.ToLower(lbName)
}

func servicePort(swarmPortCfg *dockerswarm.PortConfig) v1.ServicePort {
	pn := portName(&swarmPortCfg.PublishedPort)
	return v1.ServicePort{
		Name: pn,
		Port: int32(swarmPortCfg.PublishedPort),
		// The K8s API server sets this field on service creation. By setting
		// this ourselves, we prevent the discoverer from thinking it needs to
		// perform an update every time it compares the translated object with
		// the one that exists in gimbal.
		TargetPort: intstr.FromInt(int(swarmPortCfg.PublishedPort)),
		Protocol:   v1.ProtocolTCP, // only support TCP
	}
}

func portName(p *uint32) string {
	port := strconv.Itoa(int(*p))
	return "port-" + port
}

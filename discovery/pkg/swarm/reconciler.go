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
	"fmt"
	"time"

	dockerswarm "github.com/docker/docker/api/types/swarm"
	localmetrics "github.com/heptio/gimbal/discovery/pkg/metrics"
	"github.com/heptio/gimbal/discovery/pkg/sync"
	"github.com/heptio/gimbal/discovery/pkg/translator"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// SwarmServiceLister lists all the swarm services in a cluster
type SwarmServiceLister interface {
	ListSwarmServices() ([]dockerswarm.Service, error)
	ListSwarmServiceNodes(dockerswarm.Service) (map[string]bool, error)
	ListSwarmServicePorts(dockerswarm.Service) (map[uint32]bool, error)
}

// The Reconciler connects to an OpenStack cluster and makes sure that the Load
// Balancers defined in the cluster are reflected in the Gimbal Kubernetes
// cluster as Services and Endpoints. The Reconciler runs on a configurable
// interval.
type Reconciler struct {
	// BackendName is the name of the Swarm cluster
	BackendName string
	ClusterType string
	// GimbalKubeClient is the client of the Kubernetes cluster where Gimbal is running
	GimbalKubeClient kubernetes.Interface
	// Interval between reconciliation loops
	SyncPeriod time.Duration
	SwarmServiceLister
	Logger    *logrus.Logger
	syncqueue sync.Queue

	Metrics localmetrics.DiscovererMetrics
}

// Endpoints represents a v1.Endpoints + upstream name to facilicate metrics
type Endpoints struct {
	endpoints    v1.Endpoints
	upstreamName string
}

// NewReconciler returns an OpenStack reconciler
func NewReconciler(backendName string, gimbalKubeClient kubernetes.Interface,
	syncPeriod time.Duration, swarmLister SwarmServiceLister, log *logrus.Logger, queueWorkers int, metrics localmetrics.DiscovererMetrics) Reconciler {

	return Reconciler{
		BackendName:        backendName,
		GimbalKubeClient:   gimbalKubeClient,
		SyncPeriod:         syncPeriod,
		SwarmServiceLister: swarmLister,
		Logger:             log,
		Metrics:            metrics,
		syncqueue:          sync.NewQueue(log, gimbalKubeClient, queueWorkers, metrics),
	}
}

// Run starts the reconciler
func (r *Reconciler) Run(stop <-chan struct{}) {
	go r.syncqueue.Run(stop)

	ticker := time.NewTicker(r.SyncPeriod)
	defer ticker.Stop()

	// Perform an initial reconciliation
	r.reconcile()

	// Perform reconciliation on every tick
	for {
		select {
		case <-stop:
			r.Logger.Info("Stopping Swarm reconciler")
			return
		case <-ticker.C:
			r.reconcile()
		}
	}
}

func (r *Reconciler) reconcile() {
	// Calculate cycle time
	// start := time.Now()
	log := r.Logger
	log.Info("reconciling Swarm services")

	// Get all services and endpoints that exist in the corresponding namespace
	clusterLabelSelector := fmt.Sprintf("%s=%s", translator.GimbalLabelBackend, r.BackendName)
	currentServices, err := r.GimbalKubeClient.CoreV1().Services(r.BackendName).List(metav1.ListOptions{LabelSelector: clusterLabelSelector})
	if err != nil {
		// r.Metrics.GenericMetricError("ListServicesInNamespace")
		log.Errorf("error listing services in namespace %q: %v", r.BackendName, err)
	}

	currentk8sEndpoints, err := r.GimbalKubeClient.CoreV1().Endpoints(r.BackendName).List(metav1.ListOptions{LabelSelector: clusterLabelSelector})
	if err != nil {
		// r.Metrics.GenericMetricError("ListEndpointsInNamespace")
		log.Errorf("error listing endpoints in namespace:%q: %v", r.BackendName, err)
	}

	// Convert the k8s list to type []Endpoints so make comparison easier
	currentEndpoints := []Endpoints{}
	for _, v := range currentk8sEndpoints.Items {
		currentEndpoints = append(currentEndpoints, Endpoints{endpoints: v, upstreamName: ""})
	}

	// Reconcile current state with desired state
	desiredSvcs := kubeServices(r.BackendName, r.SwarmServiceLister)
	r.reconcileSvcs(desiredSvcs, currentServices.Items)

	desiredEndpoints := kubeEndpoints(r.BackendName, r.SwarmServiceLister)
	r.reconcileEndpoints(desiredEndpoints, currentEndpoints)

	// Log upstream /invalid services to prometheus
	// r.Metrics.DiscovererUpstreamServicesMetric(projectName, totalUpstreamServices)
	// r.Metrics.DiscovererInvalidServicesMetric(projectName, totalInvalidServices)

	// for _, ep := range desiredEndpoints {
	//	 totalUpstreamEndpoints := sync.SumEndpoints(&ep.endpoints)
	//	 r.Metrics.DiscovererUpstreamEndpointsMetric(r.BackendName, ep.upstreamName, totalUpstreamEndpoints)
	// }

	// Log to Prometheus the cycle duration
	// r.Metrics.CycleDurationMetric(time.Now().Sub(start))
}

func (r *Reconciler) reconcileSvcs(desiredSvcs, currentSvcs []v1.Service) {
	add, up, del := diffServices(desiredSvcs, currentSvcs)
	for _, svc := range add {
		s := svc
		r.syncqueue.Enqueue(sync.AddServiceAction(&s))
	}
	for _, svc := range up {
		s := svc
		r.syncqueue.Enqueue(sync.UpdateServiceAction(&s))
	}
	for _, svc := range del {
		s := svc
		r.syncqueue.Enqueue(sync.DeleteServiceAction(&s))
	}
}

func (r *Reconciler) reconcileEndpoints(desired []Endpoints, current []Endpoints) {
	add, up, del := diffEndpoints(desired, current)
	for _, ep := range add {
		e := ep
		r.syncqueue.Enqueue(sync.AddEndpointsAction(&e.endpoints, e.upstreamName))
	}
	for _, ep := range up {
		e := ep
		r.syncqueue.Enqueue(sync.UpdateEndpointsAction(&e.endpoints, e.upstreamName))
	}
	for _, ep := range del {
		e := ep
		r.syncqueue.Enqueue(sync.DeleteEndpointsAction(&e.endpoints, e.upstreamName))
	}
}

func contains(s []string, e string) bool {
	for _, v := range s {
		if e == v {
			return true
		}
	}
	return false
}

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

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/docker/go-connections/tlsconfig"
	"github.com/heptio/gimbal/discovery/pkg/buildinfo"
	"github.com/heptio/gimbal/discovery/pkg/k8s"
	localmetrics "github.com/heptio/gimbal/discovery/pkg/metrics"
	"github.com/heptio/gimbal/discovery/pkg/signals"
	"github.com/heptio/gimbal/discovery/pkg/swarm"
	"github.com/heptio/gimbal/discovery/pkg/util"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

var (
	printVersion                  bool
	gimbalKubeCfgFile             string
	backendName                   string
	swarmEndpoint                 string
	numProcessThreads             int
	debug                         bool
	reconciliationPeriod          time.Duration
	httpClientTimeout             time.Duration
	swarmCertificateAuthorityFile string
	swarmClientKey                string
	swarmClientCert               string
	prometheusListenPort          int
	discovererMetrics             localmetrics.DiscovererMetrics
	log                           *logrus.Logger
	gimbalKubeClientQPS           float64
	gimbalKubeClientBurst         int
)

var reconciler swarm.Reconciler

const (
	clusterType = "swarm"
)

func init() {
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
	flag.StringVar(&gimbalKubeCfgFile, "gimbal-kubecfg-file", "", "Location of kubecfg file for access to gimbal system kubernetes api, defaults to service account tokens")
	flag.StringVar(&backendName, "backend-name", "", "Name of cluster (must be unique)")
	flag.StringVar(&swarmEndpoint, "swarm-endpoint", "", "Swarm cluster endpoint.")
	flag.IntVar(&numProcessThreads, "num-threads", 2, "Specify number of threads to use when processing queue items.")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging.")
	flag.DurationVar(&reconciliationPeriod, "reconciliation-period", 30*time.Second, "The interval of time between reconciliation loop runs.")
	flag.DurationVar(&httpClientTimeout, "http-client-timeout", 5*time.Second, "The HTTP client request timeout.")
	flag.StringVar(&swarmCertificateAuthorityFile, "swarm-certificate-authority", "", "Path to cert file of the Swarm API certificate authority.")
	flag.StringVar(&swarmClientKey, "swarm-client-key", "", "Path to key file of the Swarm client.")
	flag.StringVar(&swarmClientCert, "swarm-client-cert", "", "Path to cert file of the Swarm client.")
	flag.IntVar(&prometheusListenPort, "prometheus-listen-address", 8080, "The address to listen on for Prometheus HTTP requests")
	flag.Float64Var(&gimbalKubeClientQPS, "gimbal-client-qps", 5, "The maximum queries per second (QPS) that can be performed on the Gimbal Kubernetes API server")
	flag.IntVar(&gimbalKubeClientBurst, "gimbal-client-burst", 10, "The maximum number of queries that can be performed on the Gimbal Kubernetes API server during a burst")
	flag.Parse()
}

func main() {
	if printVersion {
		fmt.Println("swarm-discoverer")
		fmt.Printf("Version: %s\n", buildinfo.Version)
		fmt.Printf("Git commit: %s\n", buildinfo.GitSHA)
		fmt.Printf("Git tree state: %s\n", buildinfo.GitTreeState)
		os.Exit(0)
	}

	log = logrus.New()
	log.Formatter = util.GetFormatter()
	if debug {
		log.Level = logrus.DebugLevel
	}

	log.Info("Gimbal Swarm Discoverer Starting up...")
	log.Infof("Version: %s", buildinfo.Version)
	log.Infof("Backend name: %s", backendName)
	log.Infof("Number of queue worker threads: %d", numProcessThreads)
	log.Infof("Reconciliation period: %v", reconciliationPeriod)
	log.Infof("Gimbal kubernetes client QPS: %v", gimbalKubeClientQPS)
	log.Infof("Gimbal kubernetes client burst: %d", gimbalKubeClientBurst)

	// Init prometheus metrics
	discovererMetrics = localmetrics.NewMetrics("swarm", backendName)
	discovererMetrics.RegisterPrometheus(true)

	// Log info metric
	discovererMetrics.DiscovererInfoMetric(buildinfo.Version)

	// Validate cluster name present
	if backendName == "" {
		log.Fatalf("The Swarm cluster name must be provided using the `--backend-name` flag")
	}
	// Validate cluster name format
	if util.IsInvalidBackendName(backendName) {
		log.Fatalf("The Swarm cluster name is invalid.  Valid names must contain only lowercase letters, numbers, and hyphens ('-').  They must start with a letter, and must not end with a hyphen")
	}
	log.Infof("BackendName is: %s", backendName)

	gimbalKubeClient, err := k8s.NewClientWithQPS(gimbalKubeCfgFile, log, float32(gimbalKubeClientQPS), gimbalKubeClientBurst)
	if err != nil {
		log.Fatal("Failed to create kubernetes client", err)
	}

	transport := &swarm.LogRoundTripper{
		RoundTripper: http.DefaultTransport,
		Log:          log,
		BackendName:  backendName,
		Metrics:      &discovererMetrics,
	}

	if swarmCertificateAuthorityFile != "" && swarmClientKey != "" && swarmClientCert != "" {
		transport.RoundTripper = httpTransportWithTLS(log, swarmCertificateAuthorityFile, swarmClientKey, swarmClientCert)
	}

	httpClient := http.Client{
		Transport: transport,
		Timeout:   httpClientTimeout,
	}

	// EDIT ME TO BE PARAMETERISED
	// Create and configure client
	// swarmClient, err := swarm.NewSwarmClient("1.39", "tcp://34.221.253.125:6443", httpClient)
	swarmClient, err := swarm.NewSwarmClient("1.39", swarmEndpoint, &httpClient)
	if err != nil {
		log.Fatalf("Failed to create Docker client: %v", err)
	}

	reconciler = swarm.NewReconciler(
		backendName,
		gimbalKubeClient,
		reconciliationPeriod,
		swarmClient,
		log,
		numProcessThreads,
		discovererMetrics,
	)
	stopCh := signals.SetupSignalHandler()

	go func() {
		// Expose the registered metrics via HTTP.
		http.Handle("/metrics", promhttp.HandlerFor(discovererMetrics.Registry, promhttp.HandlerOpts{}))
		srv := &http.Server{Addr: fmt.Sprintf(":%d", prometheusListenPort)}
		log.Info("Listening for Prometheus metrics on port: ", prometheusListenPort)
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
		<-stopCh
		log.Info("Shutting down Prometheus server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	log.Info("Starting reconciler")
	go reconciler.Run(stopCh)

	go func() {
		http.HandleFunc("/healthz", healthzHandler)
		log.Fatal(http.ListenAndServe("127.0.0.1:8000", nil))
		<-stopCh
		log.Info("Shutting down healthz endpoint...")
	}()

	<-stopCh
	log.Info("Stopped Swarm discoverer")
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	_, err := reconciler.SwarmServiceLister.ListSwarmServices()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "FAIL")
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK")
}

func httpTransportWithTLS(log *logrus.Logger, caFile, keyFile, certFile string) http.RoundTripper {
	_, err := ioutil.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Error reading certificate authority for Swarm: %v", err)
	}
	_, err = ioutil.ReadFile(keyFile)
	if err != nil {
		log.Fatalf("Error reading client key for Swarm: %v", err)
	}
	_, err = ioutil.ReadFile(certFile)
	if err != nil {
		log.Fatalf("Error reading client certificate for Swarm: %v", err)
	}

	opts := tlsconfig.Options{
		CAFile:             caFile,
		CertFile:           certFile,
		KeyFile:            keyFile,
		ExclusiveRootPools: true,
	}
	config, err := tlsconfig.Client(opts)
	if err != nil {
		log.Fatalf("Failed to create TLS config: ", err)
	}

	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       config,
	}
}

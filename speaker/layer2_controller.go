// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"crypto/sha256"
	"maps"
	"net"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"

	"go.universe.tf/metallb/internal/config"
	"go.universe.tf/metallb/internal/k8s/epslices"
	k8snodes "go.universe.tf/metallb/internal/k8s/nodes"
	"go.universe.tf/metallb/internal/layer2"
)

type layer2Controller struct {
	announcer       *layer2.Announce
	myNode          string
	ignoreExcludeLB bool
	sList           SpeakerList
	onStatusChange  func(types.NamespacedName)
	nodes           map[string]*v1.Node
}

func (c *layer2Controller) SetConfig(log.Logger, *config.Config) error {
	return nil
}

// nodesWithEndpoint returns all nodes that have at least one fully ready
// endpoint on them and that have a speaker according to the speaker map param.
func nodesWithEndpoint(eps []discovery.EndpointSlice, speakers map[string]bool) []string {
	usable := map[string]bool{}
	for _, slice := range eps {
		for _, ep := range slice.Endpoints {
			if !epslices.EndpointCanServe(ep.Conditions) {
				continue
			}
			if ep.NodeName == nil {
				continue
			}
			nodeName := *ep.NodeName
			if hasSpeaker := speakers[nodeName]; !hasSpeaker {
				continue
			}
			if _, ok := usable[nodeName]; !ok {
				usable[nodeName] = true
			}
		}
	}

	var ret []string
	for node, ok := range usable {
		if ok {
			ret = append(ret, node)
		}
	}

	return ret
}

func (c *layer2Controller) ShouldAnnounce(l log.Logger, name string, toAnnounce []net.IP, pool *config.Pool, svc *v1.Service, eps []discovery.EndpointSlice, nodes map[string]*v1.Node) string {
	if !activeEndpointExists(eps) { // no active endpoints, just return
		level.Debug(l).Log("event", "shouldannounce", "protocol", "l2", "message", "failed no active endpoints", "service", name)
		return "notOwner"
	}

	if !poolMatchesNodeL2(pool, c.myNode) {
		level.Debug(l).Log("event", "skipping should announce l2", "service", name, "reason", "pool not matching my node")
		return "notOwner"
	}

	adsForService := l2AdsForService(pool.L2Advertisements, c.myNode, svc)
	if len(adsForService) == 0 {
		level.Debug(l).Log("event", "skipping should announce l2", "service", name, "reason", "no advertisement matching service on my node")
		return "noMatchingAdvertisement"
	}

	speakerMap := c.speakersForPool(l, name, pool, nodes)
	availableNodes := nodesWithActiveSpeakers(speakerMap)
	if svc.Spec.ExternalTrafficPolicy == v1.ServiceExternalTrafficPolicyTypeLocal {
		availableNodes = nodesWithEndpoint(eps, speakerMap)
	}

	if len(availableNodes) == 0 {
		level.Debug(l).Log("event", "skipping should announce l2", "service", name, "reason", "no available nodes")
		return "notOwner"
	}

	preferredNodes := getPreferredNodes(svc)
	for i, ip := range toAnnounce {
		if ipOwner(c.myNode, i, ip, preferredNodes, availableNodes, name) {
			return "" // Win if we own at least one IP
		}
	}

	return "notOwner"
}

func (c *layer2Controller) SetBalancer(l log.Logger, name string, lbIPs []net.IP, pool *config.Pool, client service, svc *v1.Service) error {
	ifs := c.announcer.GetInterfaces()
	adsForService := l2AdsForService(pool.L2Advertisements, c.myNode, svc)

	// Use our local nodes map to avoid panic and get correct available nodes.
	speakerMap := c.speakersForPool(l, name, pool, c.nodes)
	availableNodes := nodesWithActiveSpeakers(speakerMap)
	// For Cluster traffic policy, this is enough. 
	// Note: for 'Local', we'd need endpoints here too, but MetalLB calls 
	// SetBalancer only after ShouldAnnounce has already validated availability.

	preferredNodes := getPreferredNodes(svc)
	var myDesiredIPs []net.IP
	for i, ip := range lbIPs {
		if ipOwner(c.myNode, i, ip, preferredNodes, availableNodes, name) {
			myDesiredIPs = append(myDesiredIPs, ip)
		}
	}

	// Important: 'name' passed from main.go is already "namespace/name"
	svcNamespacedName := types.NamespacedName{Namespace: svc.Namespace, Name: svc.Name}
	currentAdvs := c.announcer.GetStatus(svcNamespacedName)

	updateStatus := false

	// 1. Remove IPs that we no longer own (or that were removed from service)
	for _, oldAdv := range currentAdvs {
		oldIP := oldAdv.GetIP()
		isStillDesired := false
		for _, desiredIP := range myDesiredIPs {
			if oldIP.Equal(desiredIP) {
				isStillDesired = true
				break
			}
		}
		if !isStillDesired {
			level.Info(l).Log("event", "removingStaleIP", "ip", oldIP.String(), "msg", "IP no longer belongs to this node, removing announcement")
			c.announcer.RemoveIP(name, oldIP)
			updateStatus = true
		}
	}

	// 2. Add or update IPs that we DO own.
	for _, lbIP := range myDesiredIPs {
		ipAdv := ipAdvertisementFor(lbIP, adsForService)
		if !ipAdv.MatchInterfaces(ifs...) {
			level.Warn(l).Log("op", "SetBalancer", "protocol", "layer2", "service", name, "IPAdvertisement", ipAdv,
				"localIfs", ifs, "msg", "the specified interfaces used to announce LB IP don't exist")
			client.Errorf(svc, "announceFailed", "the interfaces specified by LB IP %q doesn't exist in assigned node %q with protocol %q", lbIP.String(), c.myNode, config.Layer2)
			continue
		}
		c.announcer.SetBalancer(name, ipAdv)
		updateStatus = true
	}

	if updateStatus {
		c.onStatusChange(svcNamespacedName)
	}
	return nil
}

func (c *layer2Controller) DeleteBalancer(l log.Logger, name, reason string) error {
	if !c.announcer.AnnounceName(name) {
		return nil
	}
	c.announcer.DeleteBalancer(name)

	svcNamespace, svcName, err := cache.SplitMetaNamespaceKey(name)
	if err != nil {
		level.Warn(l).Log("op", "DeleteBalancer", "protocol", "layer2", "service", name, "msg", "failed to split key", "err", err)
		return err
	}
	c.onStatusChange(types.NamespacedName{Name: svcName, Namespace: svcNamespace})
	return nil
}

func (c *layer2Controller) SetNode(l log.Logger, n *v1.Node) error {
	c.nodes[n.Name] = n
	if c.myNode != n.Name {
		return nil
	}
	c.sList.Rejoin()
	return nil
}

func (c *layer2Controller) SetEventCallback(callback func(interface{})) {
	// Do nothing
}

func ipAdvertisementFor(ip net.IP, l2Advertisements []*config.L2Advertisement) layer2.IPAdvertisement {
	ifs := sets.Set[string]{}
	for _, l2 := range l2Advertisements {
		if l2.AllInterfaces {
			return layer2.NewIPAdvertisement(ip, true, sets.Set[string]{})
		}
		ifs = ifs.Insert(l2.Interfaces...)
	}
	return layer2.NewIPAdvertisement(ip, false, ifs)
}

// nodesWithActiveSpeakers returns the list of nodes with active speakers.
func nodesWithActiveSpeakers(speakers map[string]bool) []string {
	var ret []string
	for node := range speakers {
		ret = append(ret, node)
	}
	return ret
}

// activeEndpointExists returns true if at least one endpoint is active.
func activeEndpointExists(eps []discovery.EndpointSlice) bool {
	for _, slice := range eps {
		for _, ep := range slice.Endpoints {
			if !epslices.EndpointCanServe(ep.Conditions) {
				continue
			}
			return true
		}
	}
	return false
}

func poolMatchesNodeL2(pool *config.Pool, node string) bool {
	for _, adv := range pool.L2Advertisements {
		if adv.Nodes[node] {
			return true
		}
	}
	return false
}

// l2AdvsForService returns the L2 advertisements matching the service on the given node.
func l2AdsForService(ads []*config.L2Advertisement, node string, svc *v1.Service) []*config.L2Advertisement {
	var result []*config.L2Advertisement
	svcLabels := labels.Set(svc.Labels)
	for _, ad := range ads {
		if !ad.Nodes[node] {
			continue
		}

		if len(ad.ServiceSelectors) == 0 {
			result = append(result, ad)
			continue
		}

		for _, sel := range ad.ServiceSelectors {
			if sel.Matches(svcLabels) {
				result = append(result, ad)
				break
			}
		}
	}
	return result
}

func (c *layer2Controller) speakersForPool(l log.Logger, name string, pool *config.Pool, nodes map[string]*v1.Node) map[string]bool {
	sl := c.sList.UsableSpeakers()
	eligibleNodes := maps.Keys(sl.Nodes)
	if sl.Disabled {
		// when memberlist is disabled we consider all nodes eligible under the assumption that
		// there is a speaker pod running on all nodes.
		eligibleNodes = maps.Keys(nodes)
	}
	res := map[string]bool{}
	for s := range eligibleNodes {
		if k8snodes.IsNetworkUnavailable(nodes[s]) {
			level.Debug(l).Log("event", "skipping should announce l2", "service", name, "reason", "speaker's node has NodeNetworkUnavailable condition")
			continue
		}

		if !c.ignoreExcludeLB && k8snodes.IsNodeExcludedFromBalancers(nodes[s]) {
			level.Debug(l).Log("event", "skipping should announce l2", "service", name, "reason", "speaker's node has labeled 'node.kubernetes.io/exclude-from-external-load-balancers'")
			continue
		}

		if poolMatchesNodeL2(pool, s) {
			res[s] = true
		}
	}
	return res
}

func getPreferredNodes(svc *v1.Service) []string {
	if svc.Annotations == nil {
		return nil
	}
	val, ok := svc.Annotations["metallb.io/preferred-nodes"]
	if !ok {
		return nil
	}
	nodes := strings.Split(val, ",")
	for i := range nodes {
		nodes[i] = strings.TrimSpace(nodes[i])
	}
	return nodes
}

func ipOwner(myNode string, index int, ip net.IP, preferredNodes []string, availableNodes []string, serviceName string) bool {
	// 1. Check preferred nodes
	if index < len(preferredNodes) && preferredNodes[index] != "" {
		pref := preferredNodes[index]
		// Is the preferred node alive?
		alive := false
		for _, n := range availableNodes {
			if n == pref {
				alive = true
				break
			}
		}
		if alive {
			return myNode == pref
		}
		// Fallback to stable hashing if preferred node is dead
	}

	// 2. Stable Consistent Hashing (Rendezvous Hashing)
	// We calculate a score for each node based on the IP address.
	// The node with the highest score wins. This is stable: if a node fails,
	// only the IPs it owned will move.
	var (
		bestNode string
		maxScore []byte
	)

	ipString := ip.String()
	for _, node := range availableNodes {
		h := sha256.Sum256([]byte(node + "#" + ipString))
		score := h[:]
		if maxScore == nil || bytes.Compare(score, maxScore) > 0 {
			maxScore = score
			bestNode = node
		}
	}

	return bestNode == myNode
}


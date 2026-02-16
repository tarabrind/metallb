// SPDX-License-Identifier:Apache-2.0

package ipfamily // import "go.universe.tf/metallb/internal/ipfamily"

import (
	"fmt"
	"net"

	v1 "k8s.io/api/core/v1"
)

// Family helps identifying single stack IPv4/IPv6 vs Dual-stack ["IPv4", "IPv6"] or ["IPv6", "Ipv4"].
type Family string

func (f Family) String() string {
	return string(f)
}

const (
	IPv4      Family = "ipv4"
	IPv6      Family = "ipv6"
	DualStack Family = "dual"
	Unknown   Family = "unknown"
)

// ForAddresses returns the address family given list of addresses strings.
func ForAddresses(ips []string) (Family, error) {
	if len(ips) == 0 {
		return Unknown, fmt.Errorf("IPFamilyForAddresses: no addresses provided")
	}

	hasIPv4 := false
	hasIPv6 := false

	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			return Unknown, fmt.Errorf("IPFamilyForAddresses: Invalid address %q", ipStr)
		}
		if ip.To4() != nil {
			hasIPv4 = true
		} else {
			hasIPv6 = true
		}
	}

	if hasIPv4 && hasIPv6 {
		return DualStack, nil
	}
	if hasIPv4 {
		return IPv4, nil
	}
	return IPv6, nil
}

// ForAddressesIPs returns the address family from a given list of addresses IPs.
func ForAddressesIPs(ips []net.IP) (Family, error) {
	ipsStrings := []string{}

	for _, ip := range ips {
		ipsStrings = append(ipsStrings, ip.String())
	}
	return ForAddresses(ipsStrings)
}

// ForCIDR returns the address family from a given CIDR.
func ForCIDR(cidr *net.IPNet) Family {
	if cidr.IP.To4() == nil {
		return IPv6
	}
	return IPv4
}

// ForAddress returns the address family for a given address.
func ForAddress(ip net.IP) Family {
	if ip.To4() == nil {
		return IPv6
	}
	return IPv4
}

// ForService returns the address family of a given service.
func ForService(svc *v1.Service) (Family, error) {
	if len(svc.Spec.ClusterIPs) > 0 {
		return ForAddresses(svc.Spec.ClusterIPs)
	}
	// fallback to clusterip if clusterips are not set
	addresses := []string{svc.Spec.ClusterIP}
	return ForAddresses(addresses)
}

// SPDX-License-Identifier:Apache-2.0

package allocator

import (
	"net"
	"testing"

	"go.universe.tf/metallb/internal/config"
	"go.universe.tf/metallb/internal/ipfamily"
)

func TestAllocateMulti(t *testing.T) {
	alloc := New(noopCallback)
	alloc.SetPools(&config.Pools{ByName: map[string]*config.Pool{
		"test": {
			Name:       "test",
			AutoAssign: true,
			CIDR: []*net.IPNet{
				ipnet("1.2.3.0/28"), // 16 IPs
			},
		},
	}})

	tests := []struct {
		desc     string
		svcKey   string
		count    int
		wantIPs  int
		wantErr  bool
		ipFamily ipfamily.Family
	}{
		{
			desc:     "allocate 3 IPs",
			svcKey:   "s1",
			count:    3,
			wantIPs:  3,
			ipFamily: ipfamily.IPv4,
		},
		{
			desc:     "allocate 5 more IPs",
			svcKey:   "s2",
			count:    5,
			wantIPs:  5,
			ipFamily: ipfamily.IPv4,
		},
		{
			desc:     "request too many IPs",
			svcKey:   "s3",
			count:    10, // 3+5+10 = 18 > 16
			wantErr:  true,
			ipFamily: ipfamily.IPv4,
		},
		{
			desc:     "idempotent reallocate same count",
			svcKey:   "s1",
			count:    3,
			wantIPs:  3,
			ipFamily: ipfamily.IPv4,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ips, err := alloc.AllocateMulti(test.svcKey, svc, test.ipFamily, nil, "", "", test.count)
			if test.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %s", err)
				return
			}
			if len(ips) != test.wantIPs {
				t.Errorf("Expected %d IPs, got %d", test.wantIPs, len(ips))
			}
			// Check that all IPs are in the pool
			for _, ip := range ips {
				if !ipnet("1.2.3.0/28").Contains(ip) {
					t.Errorf("IP %s not in pool", ip)
				}
			}
		})
	}
}

func TestAllocateFromPoolMulti(t *testing.T) {
	alloc := New(noopCallback)
	alloc.SetPools(&config.Pools{ByName: map[string]*config.Pool{
		"pool1": {
			Name:       "pool1",
			AutoAssign: true,
			CIDR: []*net.IPNet{
				ipnet("1.1.1.0/28"),
			},
		},
		"pool2": {
			Name:       "pool2",
			AutoAssign: true,
			CIDR: []*net.IPNet{
				ipnet("2.2.2.0/28"),
			},
		},
	}})

	t.Run("allocate from specific pool", func(t *testing.T) {
		ips, err := alloc.AllocateFromPoolMulti("s1", svc, ipfamily.IPv4, "pool2", nil, "", "", 4)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(ips) != 4 {
			t.Errorf("Expected 4 IPs, got %d", len(ips))
		}
		for _, ip := range ips {
			if !ipnet("2.2.2.0/28").Contains(ip) {
				t.Errorf("IP %s not in expected pool2", ip)
			}
		}
	})
}

package config

import "testing"

func TestResolveNetwork(t *testing.T) {
	for _, tc := range []struct{ network, hrp, want string }{
		{"auto", "j", "mainnet"},
		{"auto", "jtest", "testnet"},
		{"auto", "jregtest", "regtest"},
		{"regtest", "jregtest", "regtest"},
	} {
		got, err := ResolveNetwork(tc.network, tc.hrp)
		if err != nil || got != tc.want {
			t.Fatalf("ResolveNetwork(%q,%q)=(%q,%v), want %q", tc.network, tc.hrp, got, err, tc.want)
		}
	}
	if _, err := ResolveNetwork("mainnet", "jregtest"); err == nil {
		t.Fatal("expected network/HRP mismatch")
	}
	if got, err := ResolveUAHRP("regtest"); err != nil || got != "jregtest" {
		t.Fatalf("ResolveUAHRP(regtest)=(%q,%v)", got, err)
	}
	if got, err := ResolveNetwork("regtest", ""); err != nil || got != "regtest" {
		t.Fatalf("ResolveNetwork(regtest,empty)=(%q,%v)", got, err)
	}
}

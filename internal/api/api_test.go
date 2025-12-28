package api

import "testing"

func TestIsSafeWalletID(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"hot", true},
		{"cold", true},
		{"hot_1", true},
		{"HOT-1", true},
		{"", true}, // empty is validated elsewhere
		{"spaces bad", false},
		{"../evil", false},
		{"a/b", false},
		{"💣", false},
	}

	for _, tc := range tests {
		if got := isSafeWalletID(tc.in); got != tc.want {
			t.Fatalf("isSafeWalletID(%q)=%v want %v", tc.in, got, tc.want)
		}
	}
}

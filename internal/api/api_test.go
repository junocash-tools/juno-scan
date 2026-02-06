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

func TestParseNotesCursor(t *testing.T) {
	valid := "123:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:7"
	cursor, err := parseNotesCursor(valid)
	if err != nil {
		t.Fatalf("parseNotesCursor(valid): %v", err)
	}
	if cursor == nil {
		t.Fatalf("cursor=nil")
	}
	if got := encodeNotesCursor(*cursor); got != valid {
		t.Fatalf("roundtrip=%q want %q", got, valid)
	}

	bad := []string{
		"bad",
		"1:zzz:0",
		"-1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		"1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:-1",
	}
	for _, in := range bad {
		if _, err := parseNotesCursor(in); err == nil {
			t.Fatalf("expected parse error for %q", in)
		}
	}
}

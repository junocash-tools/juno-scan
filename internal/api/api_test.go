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
	validLegacy := "123:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:7"
	cursor, err := parseNotesCursor(validLegacy)
	if err != nil {
		t.Fatalf("parseNotesCursor(validLegacy): %v", err)
	}
	if cursor == nil {
		t.Fatalf("cursor=nil")
	}
	if cursor.Direction != noteDirectionIncoming {
		t.Fatalf("direction=%q want incoming", cursor.Direction)
	}
	if got := encodeNotesCursor(*cursor); got != validLegacy+":incoming" {
		t.Fatalf("roundtrip=%q want %q", got, validLegacy+":incoming")
	}

	validNew := validLegacy + ":outgoing"
	cursor, err = parseNotesCursor(validNew)
	if err != nil {
		t.Fatalf("parseNotesCursor(validNew): %v", err)
	}
	if cursor == nil {
		t.Fatalf("cursor=nil for validNew")
	}
	if got := encodeNotesCursor(*cursor); got != validNew {
		t.Fatalf("roundtrip=%q want %q", got, validNew)
	}

	bad := []string{
		"bad",
		"1:zzz:0",
		"-1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		"1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:-1",
		"1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0:bad",
	}
	for _, in := range bad {
		if _, err := parseNotesCursor(in); err == nil {
			t.Fatalf("expected parse error for %q", in)
		}
	}
}

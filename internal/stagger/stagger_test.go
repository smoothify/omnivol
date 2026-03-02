/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stagger_test

import (
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/smoothify/omnivol/internal/stagger"
)

func TestMinuteOffset(t *testing.T) {
	tests := []struct {
		uid  types.UID
		want int
	}{
		// First four digits: "1234" → 1234 % 60 = 34
		{uid: "1234abcd-0000-0000-0000-000000000000", want: 34},
		// First four digits: "0000" → 0 % 60 = 0
		{uid: "0000abcd-0000-0000-0000-000000000000", want: 0},
		// First four digits: "5959" → 5959 % 60 = 19
		{uid: "5959abcd-0000-0000-0000-000000000000", want: 19},
		// First four digits: "6000" → 6000 % 60 = 0
		{uid: "6000abcd-0000-0000-0000-000000000000", want: 0},
		// Non-leading digits — pull from anywhere: "abcd-1234-…" → first 4 numeric: "1234" → 34
		{uid: "abcd-1234-0000-0000-000000000000", want: 34},
		// All letters — zero-padded to "0000" → 0
		{uid: "abcdefgh", want: 0},
		// Only 2 numeric chars "42" → padded to "4200" → 4200 % 60 = 0
		{uid: "a4b2cdef", want: 0},
		// Real-world UUID shape: first 4 digits are "4b7c" → 0: "4", "b"skip, "7", "c"skip…
		// digits: "47" + zeros → "4700" → 4700 % 60 = 20
		{uid: "a4b7cdef-0000-0000-0000-000000000000", want: 20},
	}

	for _, tt := range tests {
		t.Run(string(tt.uid), func(t *testing.T) {
			got := stagger.MinuteOffset(tt.uid)
			if got != tt.want {
				t.Errorf("MinuteOffset(%q) = %d, want %d", tt.uid, got, tt.want)
			}
		})
	}
}

func TestApplyStagger(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		uid      types.UID
		wantErr  bool
		wantMin  int
	}{
		{
			name:     "replaces minute field",
			schedule: "0 * * * *",
			uid:      "1234abcd-0000-0000-0000-000000000000", // offset=34
			wantMin:  34,
		},
		{
			name:     "preserves other fields",
			schedule: "0 2 * * 0",
			uid:      "1234abcd-0000-0000-0000-000000000000", // offset=34
			wantMin:  34,
		},
		{
			name:     "wrong field count returns error",
			schedule: "0 * * *",
			uid:      "1234abcd-0000-0000-0000-000000000000",
			wantErr:  true,
		},
		{
			name:     "six fields returns error",
			schedule: "0 0 * * * *",
			uid:      "1234abcd-0000-0000-0000-000000000000",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stagger.ApplyStagger(tt.schedule, tt.uid)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ApplyStagger(%q, %q) error = %v, wantErr %v", tt.schedule, tt.uid, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			// Parse back the minute field from the first space-delimited token.
			fields := strings.Fields(got)
			if len(fields) != 5 {
				t.Fatalf("result %q has %d fields, want 5", got, len(fields))
			}
			min, parseErr := strconv.Atoi(fields[0])
			if parseErr != nil {
				t.Fatalf("minute field %q is not an integer: %v", fields[0], parseErr)
			}
			if min != tt.wantMin {
				t.Errorf("minute field = %d, want %d (full: %q)", min, tt.wantMin, got)
			}
		})
	}
}

// TestMinuteOffsetDeterminism verifies the same UID always produces the same offset.
func TestMinuteOffsetDeterminism(t *testing.T) {
	uid := types.UID("550e8400-e29b-41d4-a716-446655440000")
	first := stagger.MinuteOffset(uid)
	for i := range 100 {
		if got := stagger.MinuteOffset(uid); got != first {
			t.Fatalf("non-deterministic: iteration %d got %d, want %d", i, got, first)
		}
	}
}

// TestMinuteOffsetRange verifies the offset is always in [0, 59].
func TestMinuteOffsetRange(t *testing.T) {
	uids := []types.UID{
		"00000000-0000-0000-0000-000000000000",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
		"550e8400-e29b-41d4-a716-446655440000",
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"6ba7b811-9dad-11d1-80b4-00c04fd430c8",
		"9999999a-0000-0000-0000-000000000000",
	}
	for _, uid := range uids {
		got := stagger.MinuteOffset(uid)
		if got < 0 || got > 59 {
			t.Errorf("MinuteOffset(%q) = %d, out of range [0,59]", uid, got)
		}
	}
}

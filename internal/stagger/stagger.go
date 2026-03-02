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

// Package stagger provides deterministic per-PVC cron schedule staggering
// based on the PVC UID, matching the logic used in the legacy Kyverno policy.
package stagger

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

var nonDigit = regexp.MustCompile(`\D`)

// MinuteOffset returns a deterministic minute offset in [0, 59] derived from
// the first four numeric characters of the PVC UID.
// Algorithm mirrors the legacy Kyverno CEL expression:
//
//	parseInt(numericCharsOf(uid)[0:4]) % 60
func MinuteOffset(uid types.UID) int {
	digits := nonDigit.ReplaceAllString(string(uid), "")
	if len(digits) < 4 {
		digits = digits + strings.Repeat("0", 4-len(digits))
	}
	n, err := strconv.Atoi(digits[:4])
	if err != nil {
		return 0
	}
	return n % 60
}

// ApplyStagger replaces the minute field of a cron expression with the
// deterministic offset derived from the PVC UID.  The cron expression must
// have exactly five fields separated by spaces (standard cron — no seconds).
// Only the minute field (index 0) is modified; all other fields are preserved.
func ApplyStagger(schedule string, uid types.UID) (string, error) {
	fields := strings.Fields(schedule)
	if len(fields) != 5 {
		return "", fmt.Errorf("schedule %q must have exactly 5 fields, got %d", schedule, len(fields))
	}
	fields[0] = strconv.Itoa(MinuteOffset(uid))
	return strings.Join(fields, " "), nil
}

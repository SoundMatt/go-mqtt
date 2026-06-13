// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mqtt

import "testing"

// FuzzMatchTopic verifies that MatchTopic does not panic on arbitrary inputs.
func FuzzMatchTopic(f *testing.F) {
	// Seed corpus: common MQTT patterns
	seeds := [][2]string{
		{"#", "a"},
		{"#", "a/b/c"},
		{"+", "a"},
		{"+/+", "a/b"},
		{"a/+/c", "a/b/c"},
		{"a/#", "a/b/c/d"},
		{"a/b", "a/b"},
		{"$SYS/#", "$SYS/broker/uptime"},
		{"#", "$SYS/test"},
		{"+", "$SYS/test"},
	}
	for _, s := range seeds {
		f.Add(s[0], s[1])
	}

	f.Fuzz(func(t *testing.T, filter, topic string) {
		// Must not panic — return value is unspecified for arbitrary inputs.
		_ = MatchTopic(filter, topic)
	})
}

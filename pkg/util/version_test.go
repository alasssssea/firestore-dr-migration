package util

import (
	"reflect"
	"testing"
)

func TestParseServerVersion(t *testing.T) {
	cases := []struct {
		in      string
		want    []int
		wantErr bool
	}{
		{"3.2.22", []int{3, 2, 22}, false},
		{"6.0.5", []int{6, 0, 5}, false},
		{"6.0.5-rc1", []int{6, 0, 5}, false},
		{"4.4", []int{4, 4, 0}, false},
		{"7", []int{7, 0, 0}, false},
		{"  5.0.14  ", []int{5, 0, 14}, false},
		{"", nil, true},
		{"abc", nil, true},
	}
	for _, tc := range cases {
		got, err := ParseServerVersion(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseServerVersion(%q) expected error, got %v", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseServerVersion(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ParseServerVersion(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestVersionAtLeast(t *testing.T) {
	cases := []struct {
		v            []int
		major, minor int
		want         bool
	}{
		{[]int{3, 6, 0}, 3, 6, true},
		{[]int{3, 5, 0}, 3, 6, false},
		{[]int{4, 0, 0}, 3, 6, true},
		{[]int{3, 2, 22}, 3, 6, false},
		{[]int{6, 0, 5}, 3, 6, true},
		{[]int{2, 6, 0}, 3, 0, false},
		{[]int{}, 3, 6, false},
	}
	for _, tc := range cases {
		if got := VersionAtLeast(tc.v, tc.major, tc.minor); got != tc.want {
			t.Errorf("VersionAtLeast(%v, %d, %d) = %v, want %v", tc.v, tc.major, tc.minor, got, tc.want)
		}
	}
}

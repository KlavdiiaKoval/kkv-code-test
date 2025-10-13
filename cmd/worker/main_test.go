package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitize(t *testing.T) {
	cases := []struct {
		name string
		in   string
		out  string
	}{
		{"alphanumeric", "abc123", "abc123"},
		{"special chars", "a*b?c", "a_b_c"},
		{"empty", "", "file"},
		{"dot dash underscore", ".-_", ".-_"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := sanitize(c.in)
			assert.Equal(t, c.out, got)
		})
	}
}

func TestGetOutputDir(t *testing.T) {
	cases := []struct {
		name string
		in   string
		out  string
	}{
		{"with dir", "/foo/bar/baz.txt", "/foo/bar"},
		{"no dir", "file.txt", "."},
		{"trailing slash", "/foo/bar/", "/foo/bar"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := getOutputDir(c.in)
			assert.Equal(t, c.out, got)
		})
	}
}

func TestStringStartsWithDot(t *testing.T) {
	cases := []struct {
		in  string
		out bool
	}{
		{".hidden", true},
		{"visible", false},
		{"", false},
	}
	for _, c := range cases {
		got := StringStartsWithDot(c.in)
		assert.Equal(t, c.out, got)
	}
}

func TestIsFileStable(t *testing.T) {
	cases := []struct {
		stableCnt int
		want      bool
	}{
		{0, false},
		{1, true},
		{2, true},
	}
	for _, c := range cases {
		st := &fileState{stableCnt: c.stableCnt}
		got := isFileStable(st)
		assert.Equal(t, c.want, got)
	}
}

func TestBuildOutputPathAndQueueName(t *testing.T) {
	cases := []struct {
		name    string
		cfg     config
		path    string
		wantQ   string
		wantOut string
	}{
		{"basic", config{queueName: "q", watchOutDir: "out"}, "/tmp/foo.txt", "q-foo.txt", "out/foo.txt"},
		{"special chars", config{queueName: "q", watchOutDir: "out"}, "/tmp/f*o?.txt", "q-f_o_.txt", "out/f*o?.txt"},
		{"empty base", config{queueName: "q", watchOutDir: "out"}, "/tmp/", "q-tmp", "out/tmp"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, out := buildOutputPathAndQueueName(c.cfg, c.path)
			assert.Equal(t, c.wantQ, q)
			assert.Equal(t, c.wantOut, out)
		})
	}
}

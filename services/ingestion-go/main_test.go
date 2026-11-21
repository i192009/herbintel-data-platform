package main

import (
	"encoding/base64"
	"testing"
)

func TestFilename(t *testing.T) {
	for _, v := range []string{"../x.txt", "a\\b.pdf", "a\x00.txt", "."} {
		if _, err := safeFilename(v); err == nil {
			t.Errorf("accepted %q", v)
		}
	}
	if v, err := safeFilename("herbs.txt"); err != nil || v != "herbs.txt" {
		t.Fatal(v, err)
	}
}
func TestPrepare(t *testing.T) {
	d := Document{SourceID: "s1", Filename: "herbs.txt", Content: base64.StdEncoding.EncodeToString([]byte("synthetic reference")), Category: "reference"}
	a, err := prepare(d, "org1")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := prepare(d, "org1")
	c, _ := prepare(d, "org2")
	if a.ID != b.ID || a.ID == c.ID || len(a.Hash) != 64 {
		t.Fatal("unstable or cross-tenant identifier")
	}
	d.Filename = "bad.pdf"
	if _, err = prepare(d, "org1"); err == nil {
		t.Fatal("accepted invalid PDF")
	}
}

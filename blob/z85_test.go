package blob_test

import (
	"testing"

	"github.com/mark-rushakoff/influx-blob/blob"
)

func TestZ85EncodeAppend_HelloWorld(t *testing.T) {
	// This tests against the Z85 spec at https://rfc.zeromq.org/spec:32/Z85/
	dst := blob.Z85EncodeAppend(nil, []byte("\x86\x4F\xD2\x6F\xB5\x59\xF7\x5B"))

	if string(dst) != "HelloWorld" {
		t.Fatalf("exp HelloWorld, got %q", dst)
	}
}

func TestZ85DecodeAppend_HelloWorld(t *testing.T) {
	// This tests against the Z85 spec at https://rfc.zeromq.org/spec:32/Z85/
	dst := blob.Z85DecodeAppend(nil, []byte("HelloWorld"))

	exp := "\x86\x4F\xD2\x6F\xB5\x59\xF7\x5B"
	if string(dst) != exp {
		t.Fatalf("exp %q, got %q", exp, dst)
	}
}

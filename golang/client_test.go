package golang

import "fmt"

import "testing"

func TestHello(t *testing.T) {
	got := Hello()
	want := "Hello, world"

	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func Hello() string {
	return "Hello, world"
}

func main() {
	fmt.Println(Hello())
}

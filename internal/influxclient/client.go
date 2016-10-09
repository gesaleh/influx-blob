package influxclient

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

type Client struct {
	baseURL string
	c       *http.Client
}

func NewClient(httpURL string) *Client {
	return &Client{
		baseURL: httpURL,
		c:       &http.Client{},
	}
}

type SendOpts struct {
	Database        string
	RetentionPolicy string

	Consistency string
}

func (c *Client) SendWrite(data []byte, opts SendOpts) error {
	vals := url.Values{
		"db":        []string{opts.Database},
		"precision": []string{"s"},
	}
	if opts.RetentionPolicy != "" {
		vals.Set("rp", opts.RetentionPolicy)
	}
	if opts.Consistency != "" {
		vals.Set("consistency", opts.Consistency)
	}

	u := c.baseURL + "/write?" + vals.Encode()

	req, err := http.NewRequest("POST", u, bytes.NewReader(data))
	if err != nil {
		return err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Unexpected status %d. Body: %q", resp.StatusCode, body)
	}

	return nil
}

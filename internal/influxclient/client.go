package influxclient

import (
	"bytes"
	"encoding/json"
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

type QueryOpts struct {
	Database        string
	RetentionPolicy string
}

// ShowSeriesForPath returns all the series keys that exactly match path.
func (c *Client) ShowSeriesForPath(blobPath string, opts QueryOpts) ([]string, error) {
	q := fmt.Sprintf("SHOW SERIES FROM %q", blobPath)
	vals := url.Values{
		"q":  []string{q},
		"db": []string{opts.Database},
	}
	req, err := http.NewRequest("GET", c.baseURL+"/query?"+vals.Encode(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// nothing?
	}

	var influxResp struct {
		Results []struct {
			Series []struct {
				Values [][]string `json:"values"`
			} `json:"series"`
		} `json:"results"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&influxResp); err != nil {
		return nil, err
	}

	if len(influxResp.Results) == 0 {
		return nil, fmt.Errorf("No results found in: %s", q)
	}
	ss := influxResp.Results[0].Series
	if len(ss) == 0 {
		return nil, fmt.Errorf("No series found in: %s", q)
	}

	sks := make([]string, len(ss[0].Values))
	for i, s := range ss[0].Values {
		sks[i] = s[0]
	}

	return sks, nil
}

func (c *Client) GetSingleBlock(db, rp, path string, blockIndex int) ([]byte, error) {
	q := fmt.Sprintf("SELECT z FROM %q WHERE bi = '%d'", path, blockIndex)
	vals := url.Values{
		"q":  []string{q},
		"db": []string{db},
		"rp": []string{rp},
	}
	req, err := http.NewRequest("GET", c.baseURL+"/query?"+vals.Encode(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var influxResp struct {
		Results []struct {
			Series []struct {
				Values [][]interface{} `json:"values"`
			} `json:"series"`
		} `json:"results"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&influxResp); err != nil {
		return nil, err
	}

	if len(influxResp.Results) == 0 {
		return nil, fmt.Errorf("No results found in: %s", q)
	}
	ss := influxResp.Results[0].Series
	if len(ss) == 0 {
		return nil, fmt.Errorf("No series found in: %s", q)
	}

	z := ss[0].Values[0][1].(string)
	return []byte(z), nil
}

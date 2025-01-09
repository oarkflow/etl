package v2

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// REST reads data from a REST API
type REST struct {
	URL string
}

func (r *REST) Read() (any, error) {
	resp, err := http.Get(r.URL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch data: %s", resp.Status)
	}

	var data any
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

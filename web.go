package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"
)

var defaultTransport http.Transport

func init() {
	// Customize the Transport to have larger connection pool
	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic(fmt.Sprintf("defaultRoundTripper not an *http.Transport"))
	}
	defaultTransport = *defaultTransportPointer // dereference it to get a copy of the struct that the pointer points to
	defaultTransport.MaxIdleConns = 1000
	defaultTransport.MaxIdleConnsPerHost = 1000
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Transport: &defaultTransport,
	}
}

//EstimateHTTPHeadersSize had to create this because headers size was not counted
func EstimateHTTPHeadersSize(headers http.Header) (result int64) {
	result = 0

	for k, v := range headers {
		result += int64(len(k) + len(": \r\n"))
		for _, s := range v {
			result += int64(len(s))
		}
	}

	result += int64(len("\r\n"))

	return result
}

func doRequest(client *http.Client, path string, method string, headerList map[string]string, urlParamList map[string]string, bodyList map[string]string) (interface{}, time.Duration, int, error) {
	// fmt.Printf("param path=%s method=%s header=%+v url_param=%+v body=%+v\n", path, method, headerList, urlParamList, bodyList)
	var buf io.Reader
	if bodyList != nil {
		data := url.Values{}
		for key, val := range bodyList {
			data.Add(key, val)
		}
		buf = bytes.NewBufferString(data.Encode())
	}

	if urlParamList != nil {
		q := url.Values{}
		for k, v := range urlParamList {
			q.Add(k, v)
		}
		path += "?" + q.Encode()
	}

	req, err := http.NewRequest(method, path, buf)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("An error occured http new request %s", err)
	}
	if headerList != nil {
		for key, val := range headerList {
			req.Header.Add(key, val)
		}
	}

	if method == "POST" {
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, 0, err
	}
	duration := time.Since(start)

	if resp == nil {
		return nil, 0, 0, fmt.Errorf("empty response")
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("An error occured reading body %s", err)
	}

	var parsed interface{}

	var respSize int
	if resp.StatusCode == http.StatusOK {
		respSize = len(body) + int(EstimateHTTPHeadersSize(resp.Header))
		if len(body) > 0 {
			if err := json.Unmarshal(body, &parsed); err != nil {
				return nil, 0, 0, fmt.Errorf("json Unmarshal error %s body=%s", err, string(body))
			}
		}
	} else if resp.StatusCode == http.StatusMovedPermanently || resp.StatusCode == http.StatusTemporaryRedirect {
		respSize = int(resp.ContentLength) + int(EstimateHTTPHeadersSize(resp.Header))
	} else {
		// fmt.Println("received status code", resp.StatusCode, "from", resp.Header, "content", string(body), req)
		return nil, 0, 0, fmt.Errorf("resp status code err=%d body=%s", resp.StatusCode, string(body))
	}
	return parsed, duration, respSize, nil
}

func loadWebData(conf *SheetConf, server string, path string) (*SheetData, error) {
	// get token
	client := newHTTPClient()
	data, _, _, err := doRequest(client, server+"/member/login", "POST", nil, nil, map[string]string{
		"userid":   "aaa",
		"password": "0e34bf6fc55b3f915430107ba8d12dd9",
	})

	if err != nil {
		return nil, fmt.Errorf("get token error=%s", err)
	}
	tokendata1 := data.(map[string]interface{})
	tokendata2 := tokendata1["Token"].(map[string]interface{})
	token := tokendata2["access_token"].(string)

	if debug {
		log.Println("token:", token)
	}

	// get data.
	data, _, _, err = doRequest(client, server+path, "GET", map[string]string{
		"Authorization": "Bearer " + token,
	}, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("get data url=%s error=%s", server+path, err)
	}
	//log.Println("get shop list", spew.Sdump(data))

	result := &SheetData{}

	// get header
	for _, col := range conf.Cols {
		result.header = append(result.header, col)
	}
	sort.Slice(result.header, func(i, j int) bool {
		return result.header[i].cellIdx < result.header[j].cellIdx
	})

	// set data.
	for _, t := range data.([]interface{}) {
		d := t.(map[string]interface{})
		//
		rowData := make([]interface{}, len(result.header))

		for idx, h := range result.header {
			if temp, ok := d[h.Column]; ok {
				switch h.Format {
				case "int":
					switch temp.(type) {
					case float64:
						rowData[idx] = int(temp.(float64))
					case int:
						rowData[idx] = temp.(int)
					case bool:
						if temp.(bool) {
							rowData[idx] = 1
						} else {
							rowData[idx] = 0
						}
					case string:
						rowData[idx], _ = strconv.Atoi(temp.(string))
					}
				case "float":
					switch temp.(type) {
					case float64:
						rowData[idx] = temp.(float64)
					case string:
						rowData[idx], _ = strconv.ParseFloat(temp.(string), 64)
					}
				case "string":
					rowData[idx] = temp.(string)
				case "datetime":
					rowData[idx] = temp.(string) //timeutil.UnixTimeToString(int(temp.(float64)))
				default:
					log.Println("invalid format", h.Format)
					panic("ll")
				}
			} else {
				rowData[idx] = h.DefaultData()
			}
		}

		if debug {
			log.Println("read row ", rowData)
		}

		result.data = append(result.data, rowData)
	}

	return result, nil
}

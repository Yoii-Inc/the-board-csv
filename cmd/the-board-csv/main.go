package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.com/go-gota/gota/dataframe"
	"github.com/jeremywohl/flatten"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Env struct {
	ApiKey   string `envconfig:"API_KEY"`
	ApiToken string `envconfig:"API_TOKEN"`
}

type JsonArray []json.RawMessage

const API_BASE = "https://api.the-board.jp/v1"

func handler() error {
	err := godotenv.Load()
	if err != nil {
		fmt.Println(err.Error())
		return errors.WithStack(err)
	}
	var cfg Env

	err = envconfig.Process("", &cfg)
	if err != nil {
		return errors.WithStack(err)
	}

	client := &http.Client{}
	if err != nil {
		return errors.WithStack(err)
	}

	header := http.Header{}
	header.Set("Authorization", fmt.Sprintf("Bearer %s", cfg.ApiToken))
	header.Set("x-api-key", cfg.ApiKey)

	u, err := url.Parse(API_BASE)
	if err != nil {
		return errors.WithStack(err)
	}
	u.Path = path.Join(u.Path, "projects")

	var jsonAr JsonArray

	for p := 1; p < 100000; p += 1 {
		time.Sleep(time.Second / 2)
		fmt.Printf("page=%d\n", p)

		// ページネーション
		q := u.Query()
		q.Set("page", fmt.Sprint(p))
		q.Set("per_page", fmt.Sprint(100))
		q.Set("response_group", "medium")
		u.RawQuery = q.Encode()

		req, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			return errors.WithStack(err)
		}
		req.Header = header

		err = func() error {
			resp, err := client.Do(req)
			if err != nil {
				return errors.WithStack(err)
			}
			if resp.StatusCode == http.StatusOK {
				defer resp.Body.Close()
				bodyBytes, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return errors.WithStack(err)
				}
				var ja JsonArray
				err = json.Unmarshal(bodyBytes, &ja)
				if err != nil {
					return errors.WithStack(err)
				}
				if len(ja) == 0 {
					return errors.New("empty response")
				}
				jsonAr = append(jsonAr, ja...)
				return nil
			} else {
				return errors.New("Status Code error")
			}
		}()
		if err != nil {
			break
		}
	}

	csvFile, err := os.Create("./out/data-projects.csv")
	if err != nil {
		return errors.WithStack(err)
	}
	defer csvFile.Close()

	for i, v := range jsonAr {
		// ネストしたjsonを正規化(dynamodbでは不要?)
		flatJsonStr, err := flatten.FlattenString(string(v), "", flatten.DotStyle)
		jsonAr[i] = json.RawMessage(flatJsonStr)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	j, _ := json.Marshal(jsonAr)
	df := dataframe.ReadJSON(bytes.NewReader(j))
	fmt.Println(df)

	df.WriteCSV(csvFile)

	return nil
}

func main() {
	err := handler()
	if err != nil {
		fmt.Printf("error: %+v", err)
	}
}

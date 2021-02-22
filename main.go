package main

import (
	"os"

	es "github.com/uncharted-distil/distil/api/elastic"
	elastic "github.com/uncharted-distil/distil/api/model/storage/elastic"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "distil-updater"
	app.Version = "0.1.0"
	app.Usage = "Update metadata stored for Distil"
	app.UsageText = "distil-updater --schema=<filepath> --dataset=<filepath> --es-endpoint=<url> --es-index=<index>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "dataset",
			Value: "",
			Usage: "The dataset id to update",
		},
		cli.StringFlag{
			Name:  "prefeaturized-path",
			Value: "",
			Usage: "The path to the prefeaturized dataset",
		},
		cli.StringFlag{
			Name:  "es-endpoint",
			Value: "",
			Usage: "The Elasticsearch endpoint",
		},
	}
	app.Action = func(c *cli.Context) error {
		// parse params
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}
		if c.String("prefeaturized-path") == "" {
			return cli.NewExitError("missing commandline flag `--prefeaturized-path`", 1)
		}
		if c.String("es-endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--es-endpoint`", 1)
		}

		esEndpoint := c.String("es-endpoint")
		prefeaturizedPath := c.String("prefeaturized-path")
		dataset := c.String("dataset")

		// setup ES storage
		esClientCtor := es.NewClient(esEndpoint, true)
		storageCtor := elastic.NewMetadataStorage("datasets", true, esClientCtor)
		storage, err := storageCtor()
		if err != nil {
			return err
		}

		// pull the dataset
		ds, err := storage.FetchDataset(dataset, true, true, true)
		if err != nil {
			return err
		}

		// update the metadata
		ds.LearningDataset = prefeaturizedPath

		// store the updated dataset
		err = storage.UpdateDataset(ds)
		if err != nil {
			return err
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}

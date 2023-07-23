package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func connectToBigQueryEmulator() (*bigquery.Client, error) {
	// Atur alamat endpoint BigQuery Emulator yang berjalan di localhost dan port 8080
	emulatorEndpoint := "http://localhost:9050"

	var options []option.ClientOption
	options = append(options, option.WithEndpoint(emulatorEndpoint))
	options = append(options, option.WithoutAuthentication())
	// Buat klien BigQuery dengan opsi koneksi ke emulator

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "test", options...)
	if err != nil {
		return nil, fmt.Errorf("gagal membuat klien BigQuery: %v", err)
	}

	return client, nil
}

func createBigQueryTable(client *bigquery.Client) error {
	// Definisikan skema tabel
	schema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
		// Tambahkan kolom lain sesuai kebutuhan
	}

	// Buat objek BigQuery TableMetadata untuk menyimpan informasi tabel
	metaData := &bigquery.TableMetadata{
		Schema: schema,
	}

	// Buat referensi ke dataset dan tabel yang ingin Anda buat
	datasetRef := client.Dataset("dataset1")
	tableRef := datasetRef.Table("your_table")

	// Buat tabel baru di BigQuery
	if err := tableRef.Create(context.Background(), metaData); err != nil {
		return fmt.Errorf("gagal membuat tabel: %v", err)
	}

	fmt.Println("Tabel berhasil dibuat.")
	return nil
}

const (
	projectID = "test"
	datasetID = "dataset1"
	routineID = "routine1"
)

var ctx = context.Background()

func main() {

	client, err := connectToBigQueryEmulator()
	if err != nil {
		log.Fatalf("gagal menyambuk ke BigQuery Emulator: %v", err)
	}
	// Buat tabel di BigQuery Emulator
	// err = createBigQueryTable(client)
	// if err != nil {
	// 	log.Fatalf("gagal membuat tabel di BigQuery Emulator: %v", err)
	// }

	err = getData(client)
	if err != nil {
		log.Fatalf("gagal query ke BigQuery Emulator: %v", err)
	}

	err = insertData(client)
	if err != nil {
		log.Fatalf("gagal insert ke BigQuery Emulator: %v", err)
	}

	err = getData(client)
	if err != nil {
		log.Fatalf("gagal query #2 ke BigQuery Emulator: %v", err)
	}

	fmt.Println("masuk")
}

// ComplexType represents a complex row item
type TableA struct {
	Name string `bigquery:"name"`
	Age  int    `bigquery:"age"`
}

func getData(client *bigquery.Client) error {
	q := client.Query("SELECT * FROM dataset1.your_table")

	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("query.Read(): %w", err)
	}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println(row)
	}
	return nil
}

func insertData(client *bigquery.Client) error {
	row := &TableA{
		Name: "Tom",
		Age:  30,
	}

	// Buat referensi ke dataset dan tabel yang ingin Anda buat
	datasetRef := client.Dataset("dataset1")
	tableRef := datasetRef.Table("your_table")
	inserter := tableRef.Inserter()

	err := inserter.Put(ctx, row)
	if err != nil {
		return fmt.Errorf("insert.Read(): %w", err)
	}
	return nil
}

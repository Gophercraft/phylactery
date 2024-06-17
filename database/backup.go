package database

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/database/storage/record"
)

// create a unique backup filename that corresponds with a snapshot in time
func create_backup_filename(timestamp time.Time) string {
	timestamp = timestamp.UTC()

	timestamp_string := strings.ReplaceAll(timestamp.Format(time.RFC3339), ":", "")

	return fmt.Sprintf("backup-%s.zip", timestamp_string)
}

// Make a backup of the database as it exists currently.
func (container *Container) TakeBackup(directory string) (err error) {
	timestamp := time.Now()

	backup_path := filepath.Join(directory, create_backup_filename(timestamp))

	var file *os.File
	file, err = os.OpenFile(backup_path, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return
	}

	err = container.backup_to_writer(file)
	if err != nil {
		return
	}

	err = file.Close()
	return
}

func (container *Container) backup_to_writer(writer io.Writer) (err error) {
	tables := container.Tables()

	var table_names []string
	for table_name := range tables {
		table_names = append(table_names, table_name)
	}

	sort.Strings(table_names)

	// tables := make([]*Table, len(table_names))
	// for index, table_name := range table_names {
	// 	tables[index] = container.Table(table_name)
	// }

	// A transaction contains within it an image of the entire database container
	var snapshot *Transaction
	snapshot, err = container.NewTransaction()
	if err != nil {
		return
	}

	zip_writer := zip.NewWriter(writer)
	for _, table_name := range table_names {
		if err = container.backup_table(snapshot, table_name, zip_writer); err != nil {
			return
		}
	}

	zip_writer.Close()

	return
}

func (container *Container) backup_table(snapshot *Transaction, table_name string, zip_writer *zip.Writer) (err error) {
	table := snapshot.Table(table_name)
	table_schema := table.Schema()

	table_backup_path := fmt.Sprintf("Table/%s.json", table_name)

	var file_writer io.Writer
	file_writer, err = zip_writer.Create(table_backup_path)

	table_encoder := record.NewJSONEncoder(file_writer)

	if err = table_encoder.EncodeSchemaHeader(table_schema); err != nil {
		return
	}

	var iter_err error

	if err = table.Iterate(func(r storage.Record) (continue_iterating bool) {
		iter_err = table_encoder.EncodeRecord(r)
		if iter_err == nil {
			continue_iterating = true
		}
		return
	}); err != nil {
		return err
	}

	if iter_err != nil {
		err = iter_err
		return
	}

	return
}

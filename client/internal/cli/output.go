package cli

import (
	"fmt"
	"os"
	"text/tabwriter"
)

func PrintTable(header []string, rows [][]string) error {
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', 0)
	if len(header) > 0 {
		if err := printTableRow(writer, header); err != nil {
			return err
		}
	}
	for _, row := range rows {
		if err := printTableRow(writer, row); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func printTableRow(writer *tabwriter.Writer, values []string) error {
	for i, value := range values {
		if i > 0 {
			if _, err := fmt.Fprint(writer, "\t"); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprint(writer, value); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(writer)
	return err
}

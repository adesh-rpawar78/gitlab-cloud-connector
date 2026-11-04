package destination

import (
	"context"
	"encoding/json"
	"fmt"
)

// StdoutDestination writes formatted events to the console log stream.
type StdoutDestination struct{}

func NewStdoutDestination() *StdoutDestination {
	return &StdoutDestination{}
}

func (s *StdoutDestination) WriteBatch(_ context.Context, records []Record) error {
	for _, r := range records {
		b, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			return err
		}
		fmt.Printf("--> [DESTINATION : CONSOLE] Record ID=%s:\n%s\n\n", r.ID, string(b))
	}
	return nil
}

func (s *StdoutDestination) Close() error {
	return nil
}

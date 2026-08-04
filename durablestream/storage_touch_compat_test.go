package durablestream

import "context"

// Touch satisfies the Storage interface for the pre-handler mock while the
// storage and HTTP changes remain in separate review commits.
func (m *mockStorage) Touch(context.Context, string) error { return nil }

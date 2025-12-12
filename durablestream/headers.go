package durablestream

// This file previously exported HTTP header and query parameter constants.
// These have been moved to internal/protocol as they are implementation details.
// Users should not need to interact with HTTP headers directly when using
// the Client or Handler APIs.
//
// If you need the wire format for protocol interoperability, see PROTOCOL.md.

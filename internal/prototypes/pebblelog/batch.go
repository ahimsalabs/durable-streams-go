package pebblelog

import "strings"

// AppendRequest is the unit handed to a backend batch. Data is one protocol
// append, not an already-concatenated wire buffer. Keeping that boundary is
// important for JSON streams: fork sub-offsets and replay must still identify
// which values arrived in one request.
type AppendRequest struct {
	StreamID    string
	ContentType string
	Data        []byte
}

// Batch is a bounded group of protocol appends. The backend may commit all
// requests in one transaction, but must write each request's messages as
// separate records (and each JSON request's flattened values as one logical
// atomic group).
type Batch struct {
	Requests []AppendRequest
	Bytes    int
	JSON     bool
}

// PlanBatches groups adjacent requests until either bound is reached. It does
// not concatenate payload bytes. A content-type transition starts a new batch
// so a backend can choose a JSON-aware encoder without inspecting arbitrary
// bytes. Requests to different streams may be grouped; stream order is
// preserved because PlanBatches never reorders its input.
//
// maxMessages and maxBytes must be positive. The returned slices borrow the
// request values (including Data); callers should copy only if they retain a
// batch beyond the append call.
func PlanBatches(requests []AppendRequest, maxMessages, maxBytes int) []Batch {
	if len(requests) == 0 || maxMessages <= 0 || maxBytes <= 0 {
		return nil
	}
	result := make([]Batch, 0, (len(requests)+maxMessages-1)/maxMessages)
	var cur Batch
	flush := func() {
		if len(cur.Requests) == 0 {
			return
		}
		cur.Requests = append([]AppendRequest(nil), cur.Requests...)
		result = append(result, cur)
		cur = Batch{}
	}
	for _, req := range requests {
		isJSON := isJSONContentType(req.ContentType)
		n := len(req.Data)
		// A single oversized request is emitted on its own. The storage layer
		// still enforces its per-message limit; this planner only bounds groups.
		if len(cur.Requests) > 0 && (len(cur.Requests) >= maxMessages || cur.Bytes+n > maxBytes || cur.JSON != isJSON) {
			flush()
		}
		cur.Requests = append(cur.Requests, req)
		cur.Bytes += n
		cur.JSON = isJSON
	}
	flush()
	return result
}

func isJSONContentType(contentType string) bool {
	media := strings.ToLower(strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0]))
	return media == "application/json" || strings.HasSuffix(media, "+json")
}

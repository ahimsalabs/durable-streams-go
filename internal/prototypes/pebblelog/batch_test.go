package pebblelog

import "testing"

func TestPlanBatchesContentAndByteBounds(t *testing.T) {
	requests := []AppendRequest{
		{StreamID: "a", ContentType: "application/json", Data: []byte("{}")},
		{StreamID: "a", ContentType: "application/json; charset=utf-8", Data: []byte("[]")},
		{StreamID: "a", ContentType: "application/octet-stream", Data: []byte("binary")},
		{StreamID: "b", ContentType: "application/octet-stream", Data: []byte("x")},
	}
	got := PlanBatches(requests, 8, 64)
	if len(got) != 2 || len(got[0].Requests) != 2 || !got[0].JSON || len(got[1].Requests) != 2 || got[1].JSON {
		t.Fatalf("PlanBatches = %#v", got)
	}
	if got[0].Requests[0].Data[0] != '{' || got[0].Requests[1].Data[0] != '[' {
		t.Fatal("planner changed request payloads")
	}
}

func TestPlanBatchesSplitsByteBudget(t *testing.T) {
	got := PlanBatches([]AppendRequest{{Data: []byte("1234")}, {Data: []byte("5678")}, {Data: []byte("9")}}, 10, 8)
	if len(got) != 2 || len(got[0].Requests) != 2 || len(got[1].Requests) != 1 {
		t.Fatalf("PlanBatches = %#v", got)
	}
}

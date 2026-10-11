package script

import "testing"

func TestStepAndEmail(t *testing.T) {
	if Steps() < 3 || EmailStep() != Steps()-2 || DoneStep() != Steps()-1 {
		t.Fatalf("steps=%d email=%d done=%d", Steps(), EmailStep(), DoneStep())
	}
	if _, ok := Tool(DoneStep()); ok {
		t.Fatal("done step must not be a tool")
	}
	tc, ok := Tool(EmailStep())
	if !ok || tc.Name != "send_email" {
		t.Fatalf("email tool = %+v %v", tc, ok)
	}
	if StepOf([]map[string]any{{"role": "user"}, {"role": "assistant"}, {"role": "tool"}, {"role": "assistant"}}) != 2 {
		t.Fatal("StepOf counts assistant turns")
	}
}

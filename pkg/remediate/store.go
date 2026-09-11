package remediate

import (
	"encoding/json"
	"os"
)

// DefaultPlanFile is where the console persists the active remediation plan so it
// survives restarts and is shared between assessment (simulation) and migration
// (real application).
const DefaultPlanFile = "remediation-plan.json"

// Load reads a plan from path. A missing file yields an empty plan (not an error)
// so the first run starts clean.
func Load(path string) (*Plan, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Plan{}, nil
	}
	if err != nil {
		return nil, err
	}
	var p Plan
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// Save writes the plan to path (pretty-printed for human inspection).
func (p *Plan) Save(path string) error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

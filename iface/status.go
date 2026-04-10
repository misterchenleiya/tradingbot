package iface

import "time"

type ModuleStatus struct {
	Name      string    `json:"name"`
	State     string    `json:"state"`
	Message   string    `json:"message,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	Details   any       `json:"details,omitempty"`
}

type StatusProvider interface {
	Status() ModuleStatus
}

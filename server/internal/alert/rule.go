// Package alert implements the alert rule engine, state machine with hysteresis,
// and notification dispatch for PulseWatch.
package alert

import (
	"fmt"
	"time"
)

// AlertState represents the lifecycle state of an alert.
type AlertState string

const (
	StateNormal   AlertState = "normal"
	StatePending  AlertState = "pending"
	StateFiring   AlertState = "firing"
	StateResolved AlertState = "resolved"
)

// Rule defines an alert evaluation rule.
type Rule struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	HostID      string        `json:"host_id,omitempty"` // empty matches any host
	MetricName  string        `json:"metric_name"`
	Condition   string        `json:"condition"`          // ">", ">=", "<", "<=", "anomaly"
	Threshold   float64       `json:"threshold"`          // numerical threshold or anomaly score threshold
	Detector    string        `json:"detector,omitempty"` // "zscore", "ewma", "mad" (for anomaly condition)
	Duration    time.Duration `json:"duration"`           // duration condition must hold before firing
	Severity    string        `json:"severity"`           // "critical", "warning", "info"
	WebhookURL  string        `json:"webhook_url,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
}

// AlertInstance tracks an ongoing or recent alert for a specific host and rule.
type AlertInstance struct {
	ID           string     `json:"id"`
	RuleID       string     `json:"rule_id"`
	RuleName     string     `json:"rule_name"`
	HostID       string     `json:"host_id"`
	MetricName   string     `json:"metric_name"`
	State        AlertState `json:"state"`
	CurrentValue float64    `json:"current_value"`
	Threshold    float64    `json:"threshold"`
	Severity     string     `json:"severity"`
	Message      string     `json:"message"`
	FirstTrigger time.Time  `json:"first_trigger"`
	FiredAt      *time.Time `json:"fired_at,omitempty"`
	ResolvedAt   *time.Time `json:"resolved_at,omitempty"`
	LastUpdated  time.Time  `json:"last_updated"`
}

// Notification represents a dispatched alert message.
type Notification struct {
	AlertID   string     `json:"alert_id"`
	RuleName  string     `json:"rule_name"`
	HostID    string     `json:"host_id"`
	State     AlertState `json:"state"`
	Severity  string     `json:"severity"`
	Message   string     `json:"message"`
	Timestamp time.Time  `json:"timestamp"`
}

// EvaluateCondition returns true if val satisfies the condition against threshold.
func EvaluateCondition(val float64, condition string, threshold float64) (bool, error) {
	switch condition {
	case ">":
		return val > threshold, nil
	case ">=":
		return val >= threshold, nil
	case "<":
		return val < threshold, nil
	case "<=":
		return val <= threshold, nil
	case "==", "=":
		return val == threshold, nil
	case "!=":
		return val != threshold, nil
	case "anomaly":
		return val >= threshold, nil
	default:
		return false, fmt.Errorf("unsupported condition: %q", condition)
	}
}

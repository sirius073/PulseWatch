package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"pulsewatch/server/internal/detect"
)

// Engine orchestrates alert rules, evaluations, state machines, and notifications.
type Engine struct {
	mu            sync.RWMutex
	rules         map[string]Rule
	instances     map[string]*AlertInstance // key: ruleID + ":" + hostID
	history       []AlertInstance           // bounded history of resolved alerts
	notifications []Notification
	detectors     map[string]detect.Detector // key: ruleID + ":" + hostID
	httpClient    *http.Client
	notifyHook    func(Notification) // optional callback for testing
}

// NewEngine creates an alert engine with default settings.
func NewEngine() *Engine {
	e := &Engine{
		rules:         make(map[string]Rule),
		instances:     make(map[string]*AlertInstance),
		history:       make([]AlertInstance, 0, 100),
		notifications: make([]Notification, 0, 100),
		detectors:     make(map[string]detect.Detector),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
	e.loadDefaultRules()
	return e
}

// SetNotifyHook registers a callback called synchronously on new notifications (useful for tests).
func (e *Engine) SetNotifyHook(hook func(Notification)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.notifyHook = hook
}

// loadDefaultRules registers initial practical monitoring alert rules.
func (e *Engine) loadDefaultRules() {
	defaults := []Rule{
		{
			ID:         "cpu-high",
			Name:       "High CPU Utilization",
			MetricName: "system.cpu.total",
			Condition:  ">",
			Threshold:  90.0,
			Duration:   30 * time.Second,
			Severity:   "warning",
			CreatedAt:  time.Now(),
		},
		{
			ID:         "mem-critical",
			Name:       "Critical Memory Exhaustion",
			MetricName: "system.memory.used_percent",
			Condition:  ">",
			Threshold:  90.0,
			Duration:   30 * time.Second,
			Severity:   "critical",
			CreatedAt:  time.Now(),
		},
		{
			ID:         "disk-high",
			Name:       "High Disk Space Usage",
			MetricName: "system.disk.used_percent",
			Condition:  ">",
			Threshold:  85.0,
			Duration:   60 * time.Second,
			Severity:   "warning",
			CreatedAt:  time.Now(),
		},
		{
			ID:         "cpu-anomaly-zscore",
			Name:       "Sudden CPU Anomaly (Z-Score)",
			MetricName: "system.cpu.total",
			Condition:  "anomaly",
			Detector:   "zscore",
			Threshold:  3.0,
			Duration:   10 * time.Second,
			Severity:   "warning",
			CreatedAt:  time.Now(),
		},
	}

	for _, r := range defaults {
		e.rules[r.ID] = r
	}
}

// AddRule registers or updates a rule.
func (e *Engine) AddRule(r Rule) error {
	if r.ID == "" {
		r.ID = fmt.Sprintf("rule-%d", time.Now().UnixNano())
	}
	if r.Name == "" {
		return fmt.Errorf("rule name is required")
	}
	if r.MetricName == "" {
		return fmt.Errorf("metric_name is required")
	}
	if r.Condition == "" {
		r.Condition = ">"
	}
	if r.Severity == "" {
		r.Severity = "warning"
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules[r.ID] = r
	return nil
}

// DeleteRule removes a rule and cleans up active instances for it.
func (e *Engine) DeleteRule(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.rules[id]; !ok {
		return false
	}
	delete(e.rules, id)

	// Clean up any instances associated with this rule
	for k, inst := range e.instances {
		if inst.RuleID == id {
			delete(e.instances, k)
		}
	}
	return true
}

// ListRules returns all configured rules.
func (e *Engine) ListRules() []Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	list := make([]Rule, 0, len(e.rules))
	for _, r := range e.rules {
		list = append(list, r)
	}
	return list
}

// GetRule returns a rule by its ID.
func (e *Engine) GetRule(id string) (Rule, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.rules[id]
	return r, ok
}

// EvaluateMetric evaluates all matching rules for an incoming (hostID, metricName, value, timestamp).
func (e *Engine) EvaluateMetric(hostID, metricName string, val float64, ts time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, rule := range e.rules {
		// Rule filter by metric
		if rule.MetricName != metricName {
			continue
		}
		// Rule filter by host (if specified)
		if rule.HostID != "" && rule.HostID != hostID {
			continue
		}

		key := rule.ID + ":" + hostID

		// Evaluate condition
		var isBreached bool
		var evalScore float64

		if rule.Condition == "anomaly" {
			d, ok := e.detectors[key]
			if !ok {
				switch rule.Detector {
				case "ewma":
					d = detect.NewEWMA(0.2, rule.Threshold, 10)
				case "mad":
					d = detect.NewMAD(50, rule.Threshold, 10)
				default:
					d = detect.NewZScore(50, rule.Threshold, 10)
				}
				e.detectors[key] = d
			}
			isBreached, evalScore = d.Update(val)
		} else {
			var err error
			isBreached, err = EvaluateCondition(val, rule.Condition, rule.Threshold)
			if err != nil {
				continue
			}
			evalScore = val
		}

		e.processStateTransition(rule, hostID, val, evalScore, isBreached, ts)
	}
}

// processStateTransition executes the state machine logic for an instance.
// Caller MUST hold e.mu.
func (e *Engine) processStateTransition(
	rule Rule,
	hostID string,
	rawVal float64,
	evalVal float64,
	isBreached bool,
	ts time.Time,
) {
	key := rule.ID + ":" + hostID
	inst, exists := e.instances[key]

	if isBreached {
		if !exists || inst.State == StateNormal || inst.State == StateResolved {
			// New alert trigger
			inst = &AlertInstance{
				ID:           fmt.Sprintf("%s-%s-%d", rule.ID, hostID, ts.Unix()),
				RuleID:       rule.ID,
				RuleName:     rule.Name,
				HostID:       hostID,
				MetricName:   rule.MetricName,
				State:        StatePending,
				CurrentValue: rawVal,
				Threshold:    rule.Threshold,
				Severity:     rule.Severity,
				Message:      fmt.Sprintf("%s on %s breached threshold: %.2f (threshold: %.2f)", rule.Name, hostID, evalVal, rule.Threshold),
				FirstTrigger: ts,
				LastUpdated:  ts,
			}
			e.instances[key] = inst

			// If duration is 0, fire immediately
			if rule.Duration <= 0 {
				inst.State = StateFiring
				now := ts
				inst.FiredAt = &now
				e.dispatchNotification(*inst)
			}
		} else if inst.State == StatePending {
			inst.CurrentValue = rawVal
			inst.LastUpdated = ts
			// Check if condition has persisted longer than Duration
			if ts.Sub(inst.FirstTrigger) >= rule.Duration {
				inst.State = StateFiring
				now := ts
				inst.FiredAt = &now
				inst.Message = fmt.Sprintf("%s on %s is FIRING (persisted for >= %s): current %.2f", rule.Name, hostID, rule.Duration, rawVal)
				e.dispatchNotification(*inst)
			}
		} else if inst.State == StateFiring {
			inst.CurrentValue = rawVal
			inst.LastUpdated = ts
		}
	} else {
		// Condition returned to normal
		if exists {
			if inst.State == StatePending {
				// Flap protection: condition resolved before duration threshold; reset to normal
				delete(e.instances, key)
			} else if inst.State == StateFiring {
				// Active alert resolved!
				inst.State = StateResolved
				now := ts
				inst.ResolvedAt = &now
				inst.LastUpdated = ts
				inst.Message = fmt.Sprintf("%s on %s is RESOLVED (value returned to %.2f)", rule.Name, hostID, rawVal)
				e.dispatchNotification(*inst)

				// Archive to history and clear active instance
				e.archiveAlert(*inst)
				delete(e.instances, key)
			}
		}
	}
}

// archiveAlert preserves resolved alerts up to capacity.
func (e *Engine) archiveAlert(inst AlertInstance) {
	if len(e.history) >= 200 {
		e.history = e.history[1:]
	}
	e.history = append(e.history, inst)
}

// dispatchNotification records notification and triggers webhook if configured.
func (e *Engine) dispatchNotification(inst AlertInstance) {
	notif := Notification{
		AlertID:   inst.ID,
		RuleName:  inst.RuleName,
		HostID:    inst.HostID,
		State:     inst.State,
		Severity:  inst.Severity,
		Message:   inst.Message,
		Timestamp: time.Now(),
	}

	if len(e.notifications) >= 200 {
		e.notifications = e.notifications[1:]
	}
	e.notifications = append(e.notifications, notif)

	if e.notifyHook != nil {
		e.notifyHook(notif)
	}

	// Dispatch webhook in background goroutine if URL is present
	rule, ok := e.rules[inst.RuleID]
	if ok && rule.WebhookURL != "" {
		go e.sendWebhook(rule.WebhookURL, notif)
	}
}

func (e *Engine) sendWebhook(url string, notif Notification) {
	payload, err := json.Marshal(notif)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		log.Printf("[PulseWatch Alert] Webhook to %s failed: %v", url, err)
		return
	}
	defer resp.Body.Close()
}

// ListActiveAlerts returns alerts currently in StatePending or StateFiring.
func (e *Engine) ListActiveAlerts() []AlertInstance {
	e.mu.RLock()
	defer e.mu.RUnlock()
	list := make([]AlertInstance, 0, len(e.instances))
	for _, inst := range e.instances {
		list = append(list, *inst)
	}
	return list
}

// ListAlertHistory returns past resolved alerts.
func (e *Engine) ListAlertHistory() []AlertInstance {
	e.mu.RLock()
	defer e.mu.RUnlock()
	list := make([]AlertInstance, len(e.history))
	copy(list, e.history)
	return list
}

// ListNotifications returns sent notifications.
func (e *Engine) ListNotifications() []Notification {
	e.mu.RLock()
	defer e.mu.RUnlock()
	list := make([]Notification, len(e.notifications))
	copy(list, e.notifications)
	return list
}

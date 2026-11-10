package alert_test

import (
	"sync"
	"testing"
	"time"

	"pulsewatch/server/internal/alert"
)

func TestAlertRuleEvaluation(t *testing.T) {
	cases := []struct {
		val       float64
		cond      string
		threshold float64
		expected  bool
	}{
		{95.0, ">", 90.0, true},
		{90.0, ">", 90.0, false},
		{90.0, ">=", 90.0, true},
		{5.0, "<", 10.0, true},
		{15.0, "<=", 10.0, false},
		{42.0, "==", 42.0, true},
		{42.0, "!=", 42.0, false},
	}

	for _, tc := range cases {
		got, err := alert.EvaluateCondition(tc.val, tc.cond, tc.threshold)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != tc.expected {
			t.Errorf("EvaluateCondition(%v, %s, %v) = %v; want %v", tc.val, tc.cond, tc.threshold, got, tc.expected)
		}
	}
}

func TestAlertStateMachineHysteresis(t *testing.T) {
	engine := alert.NewEngine()

	// Register a test rule with 30s duration
	testRule := alert.Rule{
		ID:         "cpu-test",
		Name:       "CPU High Test",
		MetricName: "test.metric.hysteresis",
		Condition:  ">",
		Threshold:  80.0,
		Duration:   30 * time.Second,
		Severity:   "warning",
	}
	if err := engine.AddRule(testRule); err != nil {
		t.Fatalf("AddRule failed: %v", err)
	}

	var notifs []alert.Notification
	var mu sync.Mutex
	engine.SetNotifyHook(func(n alert.Notification) {
		mu.Lock()
		defer mu.Unlock()
		notifs = append(notifs, n)
	})

	baseTime := time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)

	// Step 1: Normal reading (< 80)
	engine.EvaluateMetric("host-1", "test.metric.hysteresis", 50.0, baseTime)
	active := engine.ListActiveAlerts()
	if len(active) != 0 {
		t.Fatalf("expected 0 active alerts, got %d", len(active))
	}

	// Step 2: First breach at t=0s -> should enter StatePending (NOT firing yet)
	engine.EvaluateMetric("host-1", "test.metric.hysteresis", 85.0, baseTime)
	active = engine.ListActiveAlerts()
	if len(active) != 1 {
		t.Fatalf("expected 1 active alert, got %d", len(active))
	}
	if active[0].State != alert.StatePending {
		t.Fatalf("expected StatePending, got %s", active[0].State)
	}
	if len(notifs) != 0 {
		t.Fatalf("expected 0 notifications during pending state, got %d", len(notifs))
	}

	// Step 3: Continued breach at t=15s (< 30s duration) -> still StatePending
	t15 := baseTime.Add(15 * time.Second)
	engine.EvaluateMetric("host-1", "test.metric.hysteresis", 88.0, t15)
	active = engine.ListActiveAlerts()
	if active[0].State != alert.StatePending {
		t.Fatalf("expected still StatePending at t=15s, got %s", active[0].State)
	}

	// Step 4: Continued breach at t=35s (>= 30s duration) -> transitions to StateFiring!
	t35 := baseTime.Add(35 * time.Second)
	engine.EvaluateMetric("host-1", "test.metric.hysteresis", 92.0, t35)
	active = engine.ListActiveAlerts()
	if active[0].State != alert.StateFiring {
		t.Fatalf("expected StateFiring at t=35s, got %s", active[0].State)
	}
	mu.Lock()
	if len(notifs) != 1 {
		t.Fatalf("expected 1 notification on firing transition, got %d", len(notifs))
	}
	if notifs[0].State != alert.StateFiring {
		t.Fatalf("expected firing notification, got %s", notifs[0].State)
	}
	mu.Unlock()

	// Step 5: Metric recovers at t=50s (value=60.0) -> transitions to StateResolved!
	t50 := baseTime.Add(50 * time.Second)
	engine.EvaluateMetric("host-1", "test.metric.hysteresis", 60.0, t50)
	active = engine.ListActiveAlerts()
	if len(active) != 0 {
		t.Fatalf("expected 0 active alerts after resolution, got %d", len(active))
	}
	history := engine.ListAlertHistory()
	if len(history) != 1 {
		t.Fatalf("expected 1 historical resolved alert, got %d", len(history))
	}
	if history[0].State != alert.StateResolved {
		t.Fatalf("expected resolved alert state, got %s", history[0].State)
	}

	mu.Lock()
	if len(notifs) != 2 {
		t.Fatalf("expected 2 notifications (firing + resolved), got %d", len(notifs))
	}
	if notifs[1].State != alert.StateResolved {
		t.Fatalf("expected resolved notification, got %s", notifs[1].State)
	}
	mu.Unlock()
}

func TestAlertFlapProtection(t *testing.T) {
	engine := alert.NewEngine()

	testRule := alert.Rule{
		ID:         "mem-flap",
		Name:       "Memory Flap Test",
		MetricName: "test.metric.flap",
		Condition:  ">",
		Threshold:  85.0,
		Duration:   20 * time.Second,
		Severity:   "warning",
	}
	_ = engine.AddRule(testRule)

	var notifs []alert.Notification
	engine.SetNotifyHook(func(n alert.Notification) {
		notifs = append(notifs, n)
	})

	t0 := time.Now()
	// Breach at t=0
	engine.EvaluateMetric("host-flap", "test.metric.flap", 90.0, t0)
	active := engine.ListActiveAlerts()
	if len(active) != 1 || active[0].State != alert.StatePending {
		t.Fatalf("expected 1 pending alert, got %v", active)
	}

	// Recovers at t=5s (before 20s duration) -> flap protection cleans up without firing
	engine.EvaluateMetric("host-flap", "test.metric.flap", 70.0, t0.Add(5*time.Second))
	active = engine.ListActiveAlerts()
	if len(active) != 0 {
		t.Fatalf("expected 0 active alerts after flap recovery, got %d", len(active))
	}
	if len(notifs) != 0 {
		t.Fatalf("flap should not trigger any notifications, got %d", len(notifs))
	}
}

func TestAlertRuleManagement(t *testing.T) {
	engine := alert.NewEngine()

	r := alert.Rule{
		ID:         "custom-1",
		Name:       "Custom Rule",
		MetricName: "system.disk.used_percent",
		Condition:  ">=",
		Threshold:  95.0,
	}

	if err := engine.AddRule(r); err != nil {
		t.Fatalf("AddRule failed: %v", err)
	}

	got, ok := engine.GetRule("custom-1")
	if !ok || got.Threshold != 95.0 {
		t.Fatalf("GetRule failed: %v, %v", ok, got)
	}

	deleted := engine.DeleteRule("custom-1")
	if !deleted {
		t.Fatal("expected DeleteRule to return true")
	}

	_, ok = engine.GetRule("custom-1")
	if ok {
		t.Fatal("rule should have been deleted")
	}
}

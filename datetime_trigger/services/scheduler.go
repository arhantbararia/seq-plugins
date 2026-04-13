package services

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"datetime_trigger/models"
	"datetime_trigger/worker"

	"github.com/google/uuid"
)

// Scheduler fires TriggerEvents based on time-based capability keys.
// One Scheduler is created per active trigger instance.
type Scheduler struct {
	TriggerID  string
	WorkflowID string
	Config         models.TriggerConfig
	Publisher      *worker.Publisher
	SequenceNumber uint64
	stopChan       chan struct{}
}

// NewScheduler creates a Scheduler for a trigger instance.
func NewScheduler(triggerID, workflowID string, config models.TriggerConfig, seq uint64, pub *worker.Publisher) *Scheduler {
	return &Scheduler{
		TriggerID:      triggerID,
		WorkflowID:     workflowID,
		Config:         config,
		Publisher:      pub,
		SequenceNumber: seq,
		stopChan:       make(chan struct{}),
	}
}

// Start launches the scheduling loop in a goroutine.
// The loop ticks every minute and checks whether the current time matches the schedule.
func (s *Scheduler) Start() {
	capKey := s.Config.CapabilityKey
	log.Printf("[Scheduler #%d] [Workflow: %s] Starting trigger=%s capability=%s config=%+v",
		s.SequenceNumber, s.WorkflowID, s.TriggerID, capKey, s.Config)

	go func() {
		// Align to the next whole minute so checks are always at :00 seconds.
		now := time.Now()
		waitUntilNextMinute := time.Duration(60-now.Second()) * time.Second
		select {
		case <-time.After(waitUntilNextMinute):
		case <-s.stopChan:
			return
		}

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		// Check immediately after alignment
		s.check(time.Now())

		for {
			select {
			case t := <-ticker.C:
				s.check(t)
			case <-s.stopChan:
				log.Printf("[Scheduler #%d] [Workflow: %s] Stopped trigger=%s", s.SequenceNumber, s.WorkflowID, s.TriggerID)
				return
			}
		}
	}()
}

// Stop signals the scheduling loop to exit.
func (s *Scheduler) Stop() {
	close(s.stopChan)
}

// check evaluates whether the given time matches the configured schedule
// and publishes a TriggerEvent if so.
func (s *Scheduler) check(t time.Time) {
	capKey := s.Config.CapabilityKey
	if !s.shouldFire(capKey, t) {
		return
	}

	event := models.TriggerEvent{
		ID:            uuid.New().String(),
		WorkflowID:    s.WorkflowID,
		TriggerID:     s.TriggerID,
		Type:          "event",
		Name:          "Date & Time Trigger",
		CapabilityKey: capKey,
		Payload: map[string]interface{}{
			"check_time": t.Format(time.RFC3339),
		},
		Timestamp: t,
	}

	if err := s.Publisher.Publish(s.WorkflowID, event); err != nil {
		log.Printf("[Scheduler #%d] [Workflow: %s] Failed to publish event trigger=%s: %v", s.SequenceNumber, s.WorkflowID, s.TriggerID, err)
	} else {
		log.Printf("[Scheduler #%d] [Workflow: %s] Fired capability=%s trigger=%s time=%s", s.SequenceNumber, s.WorkflowID, capKey, s.TriggerID, t.Format(time.RFC3339))
	}
}

// shouldFire returns true when the current minute matches the configured schedule.
func (s *Scheduler) shouldFire(capKey string, t time.Time) bool {
	switch capKey {

	// ── Every day at HH:MM ──────────────────────────────────────────────────
	// scheduled_at: "HH:MM"
	case "datetime_every_day_at":
		hhmm := strings.TrimSpace(s.Config.ScheduledAt)
		if hhmm == "" {
			log.Printf("[Scheduler #%d] [Workflow: %s] %s: missing scheduled_at", s.SequenceNumber, s.WorkflowID, capKey)
			return false
		}
		h, m, err := parseHHMM(hhmm)
		if err != nil {
			log.Printf("[Scheduler #%d] [Workflow: %s] %s: invalid scheduled_at %q: %v", s.SequenceNumber, s.WorkflowID, capKey, hhmm, err)
			return false
		}
		return t.Hour() == h && t.Minute() == m

	// ── Every hour at MM ────────────────────────────────────────────────────
	// scheduled_at: "MM" (00–59)
	case "datetime_every_hour_at":
		mmStr := strings.TrimSpace(s.Config.ScheduledAt)
		if mmStr == "" {
			log.Printf("[Scheduler] datetime_every_hour_at: missing scheduled_at")
			return false
		}
		m, err := strconv.Atoi(mmStr)
		if err != nil || m < 0 || m > 59 {
			log.Printf("[Scheduler] datetime_every_hour_at: invalid scheduled_at %q", mmStr)
			return false
		}
		return t.Minute() == m

	// ── Every day of the week at HH:MM ──────────────────────────────────────
	// day_of_week: "Monday" | "Tuesday" | … | "Sunday"
	// scheduled_at: "HH:MM"
	case "datetime_every_day_of_week_at":
		dow := strings.TrimSpace(s.Config.DayOfWeek)
		hhmm := strings.TrimSpace(s.Config.ScheduledAt)
		if dow == "" || hhmm == "" {
			log.Printf("[Scheduler] datetime_every_day_of_week_at: missing day_of_week or scheduled_at")
			return false
		}
		h, m, err := parseHHMM(hhmm)
		if err != nil {
			log.Printf("[Scheduler] datetime_every_day_of_week_at: invalid scheduled_at %q: %v", hhmm, err)
			return false
		}
		return strings.EqualFold(t.Weekday().String(), dow) && t.Hour() == h && t.Minute() == m

	// ── Every month on day D at HH:MM ───────────────────────────────────────
	// day_of_month: 1–31
	// scheduled_at: "HH:MM"
	case "datetime_every_month_on":
		dom := s.Config.DayOfMonth
		hhmm := strings.TrimSpace(s.Config.ScheduledAt)
		if dom == 0 || hhmm == "" {
			log.Printf("[Scheduler] datetime_every_month_on: missing day_of_month or scheduled_at")
			return false
		}
		h, m, err := parseHHMM(hhmm)
		if err != nil {
			log.Printf("[Scheduler] datetime_every_month_on: invalid scheduled_at %q: %v", hhmm, err)
			return false
		}
		// Clamp: if the month is shorter than dom, fire on the last day instead.
		effectiveDay := clampDayOfMonth(dom, t.Year(), t.Month())
		return t.Day() == effectiveDay && t.Hour() == h && t.Minute() == m

	// ── Every year on MM-DD at HH:MM ────────────────────────────────────────
	// scheduled_at: "MM-DD HH:MM"
	case "datetime_every_year_on":
		raw := strings.TrimSpace(s.Config.ScheduledAt)
		if raw == "" {
			log.Printf("[Scheduler] datetime_every_year_on: missing scheduled_at")
			return false
		}
		month, day, h, m, err := parseMMDDHHMM(raw)
		if err != nil {
			log.Printf("[Scheduler] datetime_every_year_on: invalid scheduled_at %q: %v", raw, err)
			return false
		}
		return int(t.Month()) == month && t.Day() == day && t.Hour() == h && t.Minute() == m

	default:
		log.Printf("[Scheduler] Unknown capability_key: %q — skipping", capKey)
		return false
	}
}

// ── helpers ─────────────────────────────────────────────────────────────────

// parseHHMM parses a "HH:MM" string and returns (hour, minute, error).
func parseHHMM(s string) (int, int, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected HH:MM, got %q", s)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h < 0 || h > 23 {
		return 0, 0, fmt.Errorf("invalid hour in %q", s)
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 0 || m > 59 {
		return 0, 0, fmt.Errorf("invalid minute in %q", s)
	}
	return h, m, nil
}

// parseMMDDHHMM parses a "MM-DD HH:MM" string.
func parseMMDDHHMM(s string) (month, day, hour, minute int, err error) {
	parts := strings.SplitN(s, " ", 2)
	if len(parts) != 2 {
		return 0, 0, 0, 0, fmt.Errorf("expected 'MM-DD HH:MM', got %q", s)
	}
	dateParts := strings.SplitN(parts[0], "-", 2)
	if len(dateParts) != 2 {
		return 0, 0, 0, 0, fmt.Errorf("expected MM-DD in date part, got %q", parts[0])
	}
	month, err = strconv.Atoi(dateParts[0])
	if err != nil || month < 1 || month > 12 {
		return 0, 0, 0, 0, fmt.Errorf("invalid month in %q", s)
	}
	day, err = strconv.Atoi(dateParts[1])
	if err != nil || day < 1 || day > 31 {
		return 0, 0, 0, 0, fmt.Errorf("invalid day in %q", s)
	}
	hour, minute, err = parseHHMM(parts[1])
	return
}

// clampDayOfMonth returns the target day, clamped to the last valid day of the given month.
// e.g. day=31 in April (30 days) → returns 30.
func clampDayOfMonth(day, year int, month time.Month) int {
	lastDay := daysInMonth(year, month)
	if day > lastDay {
		return lastDay
	}
	return day
}

// daysInMonth returns the number of days in the given month/year.
func daysInMonth(year int, month time.Month) int {
	// day=0 of next month = last day of current month
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

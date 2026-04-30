package services

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"rss_trigger/models"
	"rss_trigger/worker"

	"github.com/google/uuid"
)

// ── RSS / Atom XML structs (stdlib encoding/xml, no third-party parser) ──────

// rssFeed represents the top-level <rss> element in an RSS 2.0 feed.
type rssFeed struct {
	XMLName xml.Name   `xml:"rss"`
	Channel rssChannel `xml:"channel"`
}

type rssChannel struct {
	Title string    `xml:"title"`
	Link  string    `xml:"link"`
	Items []rssItem `xml:"item"`
}

type rssItem struct {
	Title       string        `xml:"title"`
	Link        string        `xml:"link"`
	Author      string        `xml:"author"`
	Creator     string        `xml:"creator"` // <dc:creator> — common in WordPress feeds
	Description string        `xml:"description"`
	Content     string        `xml:"encoded"` // <content:encoded>
	PubDate     string        `xml:"pubDate"`
	GUID        string        `xml:"guid"`
	Enclosure   *rssEnclosure `xml:"enclosure"`
}

type rssEnclosure struct {
	URL  string `xml:"url,attr"`
	Type string `xml:"type,attr"`
}

// atomFeed represents the top-level <feed> element in an Atom feed.
type atomFeed struct {
	XMLName xml.Name    `xml:"feed"`
	Title   string      `xml:"title"`
	Links   []atomLink  `xml:"link"`
	Entries []atomEntry `xml:"entry"`
}

type atomLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
}

type atomEntry struct {
	Title   string     `xml:"title"`
	Links   []atomLink `xml:"link"`
	Author  atomAuthor `xml:"author"`
	Content string     `xml:"content"`
	Summary string     `xml:"summary"`
	Updated string     `xml:"updated"`
	ID      string     `xml:"id"`
}

type atomAuthor struct {
	Name string `xml:"name"`
}

// ── Unified feed item ────────────────────────────────────────────────────────

// feedItem is the normalised representation shared by both RSS and Atom parsing.
type feedItem struct {
	ID        string // GUID (RSS) or ID (Atom) — used as cache key
	Title     string
	URL       string
	Author    string
	Content   string
	ImageURL  string
	Published string
	FeedTitle string
	FeedURL   string
}

// ── Poller ────────────────────────────────────────────────────────────────────

type Poller struct {
	TriggerID      string
	WorkflowID     string
	CapabilityKey  string
	TriggerConfig  models.TriggerConfig
	SequenceNumber uint64
	Publisher      *worker.Publisher
	httpClient     *http.Client
	stopChan       chan struct{}
	stopOnce       sync.Once
	mu             sync.Mutex

	// Ordered cache for change detection.
	seenCache   []string
	seenSet     map[string]bool
	isFirstPoll bool
}

func NewPoller(triggerID, workflowID string, config models.TriggerConfig, seq uint64, pub *worker.Publisher) *Poller {
	return &Poller{
		TriggerID:      triggerID,
		WorkflowID:     workflowID,
		CapabilityKey:  config.CapabilityKey,
		TriggerConfig:  config,
		SequenceNumber: seq,
		Publisher:      pub,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				TLSHandshakeTimeout: 30 * time.Second,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:          10,
				IdleConnTimeout:       90 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
		stopChan:    make(chan struct{}),
		seenCache:   make([]string, 0, 100),
		seenSet:     make(map[string]bool),
		isFirstPoll: true,
	}
}

func (p *Poller) Start() {
	log.Printf("[Poller #%d] [Workflow: %s] Starting trigger=%s capability=%s feed=%s",
		p.SequenceNumber, p.WorkflowID, p.TriggerID, p.CapabilityKey, p.TriggerConfig.FeedURL)

	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()

		p.poll()

		for {
			select {
			case <-ticker.C:
				p.poll()
			case <-p.stopChan:
				log.Printf("[Poller #%d] [Workflow: %s] Stopped trigger=%s",
					p.SequenceNumber, p.WorkflowID, p.TriggerID)
				return
			}
		}
	}()
}

func (p *Poller) Stop() {
	p.stopOnce.Do(func() { close(p.stopChan) })
}

// ── Cache management ─────────────────────────────────────────────────────────

// evictCache trims the oldest 10 entries once the cache grows past 100.
func (p *Poller) evictCache() {
	const maxCacheSize = 100
	const evictCount = 10
	if len(p.seenCache) <= maxCacheSize {
		return
	}
	n := evictCount
	if n > len(p.seenCache) {
		n = len(p.seenCache)
	}
	for i := 0; i < n; i++ {
		delete(p.seenSet, p.seenCache[i])
	}
	p.seenCache = p.seenCache[n:]
}

// ── Poll loop ────────────────────────────────────────────────────────────────

func (p *Poller) poll() {
	log.Printf("[Poller #%d] [Workflow: %s] Polling RSS for trigger=%s capability=%s",
		p.SequenceNumber, p.WorkflowID, p.TriggerID, p.CapabilityKey)

	var items []feedItem
	var err error

	switch p.CapabilityKey {
	case "rss_new_feed_item":
		items, err = p.pollFeed()
	case "rss_new_feed_item_matches":
		items, err = p.pollFeedMatches()
	default:
		log.Printf("[Poller] Unknown capability: %s", p.CapabilityKey)
		return
	}

	if err != nil {
		log.Printf("[Poller #%d] [Workflow: %s] Error polling %s: %v",
			p.SequenceNumber, p.WorkflowID, p.CapabilityKey, err)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isFirstPoll {
		// Seed cache with all returned IDs.
		for _, item := range items {
			if item.ID == "" {
				continue
			}
			if !p.seenSet[item.ID] {
				p.seenCache = append(p.seenCache, item.ID)
				p.seenSet[item.ID] = true
			}
		}
		// Fire event for the latest item (index 0) if data exists.
		// This acts as an immediate test for the workflow.
		if len(items) > 0 && items[0].ID != "" {
			p.fireEvent(items[0])
			log.Printf("[Poller #%d] [Workflow: %s] First-poll: fired event for latest item id=%s",
				p.SequenceNumber, p.WorkflowID, items[0].ID)
		}
		log.Printf("[Poller #%d] [Workflow: %s] Seeded %d initial items for trigger=%s",
			p.SequenceNumber, p.WorkflowID, len(p.seenCache), p.TriggerID)
		p.isFirstPoll = false
		return
	}

	// Subsequent polls — fire events only for items NOT already in the cache.
	newCount := 0
	for _, item := range items {
		if item.ID == "" {
			continue
		}
		if !p.seenSet[item.ID] {
			p.fireEvent(item)
			p.seenCache = append(p.seenCache, item.ID)
			p.seenSet[item.ID] = true
			newCount++
		}
	}

	// Trim cache if it grew beyond the limit.
	p.evictCache()

	if newCount > 0 {
		log.Printf("[Poller #%d] [Workflow: %s] Detected %d new feed items for trigger=%s",
			p.SequenceNumber, p.WorkflowID, newCount, p.TriggerID)
	}
}

func (p *Poller) fireEvent(item feedItem) {
	payload := map[string]interface{}{
		"entry_title":     item.Title,
		"entry_url":       item.URL,
		"entry_author":    item.Author,
		"entry_content":   item.Content,
		"entry_image_url": item.ImageURL,
		"entry_published": item.Published,
		"feed_title":      item.FeedTitle,
		"feed_url":        item.FeedURL,
	}

	event := models.TriggerEvent{
		ID:            uuid.New().String(),
		WorkflowID:    p.WorkflowID,
		TriggerID:     p.TriggerID,
		Type:          "event",
		Name:          "RSS Trigger",
		CapabilityKey: p.CapabilityKey,
		Payload:       payload,
		Timestamp:     time.Now().UTC(),
	}

	if err := p.Publisher.Publish(p.WorkflowID, event); err != nil {
		log.Printf("[Poller #%d] [Workflow: %s] Failed to publish event trigger=%s: %v",
			p.SequenceNumber, p.WorkflowID, p.TriggerID, err)
	} else {
		log.Printf("[Poller #%d] [Workflow: %s] Fired event capability=%s trigger=%s title=%q",
			p.SequenceNumber, p.WorkflowID, p.CapabilityKey, p.TriggerID, item.Title)
	}
}

// ── Capability poll functions ────────────────────────────────────────────────

// pollFeed fetches the configured RSS/Atom feed and returns all items (newest first).
// No authentication required — RSS feeds are public HTTP endpoints.
func (p *Poller) pollFeed() ([]feedItem, error) {
	feedURL := p.TriggerConfig.FeedURL
	if feedURL == "" {
		return nil, fmt.Errorf("feed_url is required for rss_new_feed_item")
	}

	return p.fetchAndParseFeed(feedURL)
}

// pollFeedMatches fetches the configured RSS/Atom feed and returns only items
// whose title or content contain the configured keyword/phrase (case-insensitive).
func (p *Poller) pollFeedMatches() ([]feedItem, error) {
	feedURL := p.TriggerConfig.FeedURL
	if feedURL == "" {
		return nil, fmt.Errorf("feed_url is required for rss_new_feed_item_matches")
	}
	keyword := p.TriggerConfig.Keyword
	if keyword == "" {
		return nil, fmt.Errorf("keyword is required for rss_new_feed_item_matches")
	}

	allItems, err := p.fetchAndParseFeed(feedURL)
	if err != nil {
		return nil, err
	}

	// Filter items that match the keyword (case-insensitive) in title or content.
	lowerKeyword := strings.ToLower(keyword)
	var matched []feedItem
	for _, item := range allItems {
		if strings.Contains(strings.ToLower(item.Title), lowerKeyword) ||
			strings.Contains(strings.ToLower(item.Content), lowerKeyword) {
			matched = append(matched, item)
		}
	}

	return matched, nil
}

// ── Feed fetching and parsing ────────────────────────────────────────────────

// fetchAndParseFeed performs an HTTP GET on the feed URL and parses the
// response as either RSS 2.0 or Atom XML. It returns a slice of feedItem
// structs in the order they appear in the feed (typically newest first for
// well-behaved feeds).
func (p *Poller) fetchAndParseFeed(feedURL string) ([]feedItem, error) {
	req, err := http.NewRequest("GET", feedURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("User-Agent", "Goat-Automate-RSS-Plugin/1.0")
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching feed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("feed returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading feed body: %w", err)
	}

	if len(body) == 0 {
		return nil, fmt.Errorf("feed returned empty body")
	}

	// Try RSS 2.0 first, then Atom.
	items, err := parseRSS(body, feedURL)
	if err == nil && len(items) > 0 {
		return items, nil
	}

	items, err = parseAtom(body, feedURL)
	if err == nil && len(items) > 0 {
		return items, nil
	}

	return nil, fmt.Errorf("could not parse feed as RSS 2.0 or Atom from %s", feedURL)
}

// parseRSS parses RSS 2.0 XML into a slice of feedItem.
func parseRSS(data []byte, feedURL string) ([]feedItem, error) {
	var rss rssFeed
	if err := xml.Unmarshal(data, &rss); err != nil {
		return nil, err
	}

	if rss.XMLName.Local != "rss" {
		return nil, fmt.Errorf("not an RSS feed")
	}

	var items []feedItem
	for _, ri := range rss.Channel.Items {
		// Build a stable ID — prefer GUID, fall back to link, then title hash.
		id := ri.GUID
		if id == "" {
			id = ri.Link
		}
		if id == "" {
			id = ri.Title
		}
		if id == "" {
			continue // skip items with no identifiable key
		}

		// Author: prefer <author>, fall back to <dc:creator>
		author := ri.Author
		if author == "" {
			author = ri.Creator
		}

		// Content: prefer <content:encoded>, fall back to <description>
		content := ri.Content
		if content == "" {
			content = ri.Description
		}

		// Image: check enclosure for image type
		imageURL := ""
		if ri.Enclosure != nil && strings.HasPrefix(ri.Enclosure.Type, "image/") {
			imageURL = ri.Enclosure.URL
		}

		items = append(items, feedItem{
			ID:        id,
			Title:     ri.Title,
			URL:       ri.Link,
			Author:    author,
			Content:   content,
			ImageURL:  imageURL,
			Published: ri.PubDate,
			FeedTitle: rss.Channel.Title,
			FeedURL:   feedURL,
		})
	}

	return items, nil
}

// parseAtom parses Atom XML into a slice of feedItem.
func parseAtom(data []byte, feedURL string) ([]feedItem, error) {
	var atom atomFeed
	if err := xml.Unmarshal(data, &atom); err != nil {
		return nil, err
	}

	if atom.XMLName.Local != "feed" {
		return nil, fmt.Errorf("not an Atom feed")
	}

	// Resolve feed-level link
	atomFeedLink := feedURL
	for _, l := range atom.Links {
		if l.Rel == "" || l.Rel == "alternate" {
			atomFeedLink = l.Href
			break
		}
	}

	var items []feedItem
	for _, ae := range atom.Entries {
		// Stable ID
		id := ae.ID
		if id == "" {
			for _, l := range ae.Links {
				if l.Rel == "" || l.Rel == "alternate" {
					id = l.Href
					break
				}
			}
		}
		if id == "" {
			id = ae.Title
		}
		if id == "" {
			continue
		}

		// Entry link
		entryURL := ""
		for _, l := range ae.Links {
			if l.Rel == "" || l.Rel == "alternate" {
				entryURL = l.Href
				break
			}
		}

		// Content: prefer <content>, fall back to <summary>
		content := ae.Content
		if content == "" {
			content = ae.Summary
		}

		items = append(items, feedItem{
			ID:        id,
			Title:     ae.Title,
			URL:       entryURL,
			Author:    ae.Author.Name,
			Content:   content,
			ImageURL:  "", // Atom feeds rarely embed images at the entry level
			Published: ae.Updated,
			FeedTitle: atom.Title,
			FeedURL:   atomFeedLink,
		})
	}

	return items, nil
}

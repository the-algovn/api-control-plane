// Package config loads and validates API registration files.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const DefaultDeadline = 5 * time.Second

var (
	reservedPrefixes = map[string]bool{"/events": true, "/healthz": true, "/metrics": true}
	prefixRe         = regexp.MustCompile(`^/[a-z0-9-]+$`)
	channelRe        = regexp.MustCompile(`^[a-z0-9-]+(\.[a-z0-9-]+)+$`)
	roleRe           = regexp.MustCompile(`^role:[a-z0-9_-]+$`)
)

// Duration parses YAML strings like "3s" via time.ParseDuration.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

type Route struct {
	Method   string   `yaml:"method"`
	Rule     string   `yaml:"rule"`
	Deadline Duration `yaml:"deadline"`
}

type Channel struct {
	Name string `yaml:"name"`
	Rule string `yaml:"rule"`
}

type Registration struct {
	Prefix      string    `yaml:"prefix"`
	Upstream    string    `yaml:"upstream"`
	DefaultRule string    `yaml:"defaultRule"`
	Routes      []Route   `yaml:"routes"`
	Channels    []Channel `yaml:"channels"`

	routeIdx map[string]Route
}

// RouteRule returns the auth rule and upstream deadline for a
// "pkg.Service/Method" string, falling back to DefaultRule.
func (r *Registration) RouteRule(method string) (string, time.Duration) {
	if rt, ok := r.routeIdx[method]; ok {
		d := time.Duration(rt.Deadline)
		if d == 0 {
			d = DefaultDeadline
		}
		return rt.Rule, d
	}
	return r.DefaultRule, DefaultDeadline
}

// HasRoute reports whether method is explicitly listed in Routes.
// Used to keep metric labels bounded by configuration.
func (r *Registration) HasRoute(method string) bool {
	_, ok := r.routeIdx[method]
	return ok
}

func ValidRule(rule string) bool {
	return rule == "anonymous" || rule == "authenticated" || roleRe.MatchString(rule)
}

type Snapshot struct {
	regs     map[string]*Registration // key: prefix
	channels map[string]string        // channel name -> rule
}

// Match resolves the registration owning a request path by its first segment.
func (s *Snapshot) Match(path string) (*Registration, bool) {
	rest := strings.TrimPrefix(path, "/")
	seg, _, _ := strings.Cut(rest, "/")
	reg, ok := s.regs["/"+seg]
	return reg, ok
}

func (s *Snapshot) ChannelRule(name string) (string, bool) {
	rule, ok := s.channels[name]
	return rule, ok
}

func (s *Snapshot) Registrations() []*Registration {
	out := make([]*Registration, 0, len(s.regs))
	for _, r := range s.regs {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Prefix < out[j].Prefix })
	return out
}

// LoadDir parses and validates every *.yaml file in dir. Any invalid file
// fails the whole load — the caller keeps its previous snapshot.
func LoadDir(dir string) (*Snapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	snap := &Snapshot{regs: map[string]*Registration{}, channels: map[string]string{}}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var reg Registration
		dec := yaml.NewDecoder(strings.NewReader(string(data)))
		dec.KnownFields(true)
		if err := dec.Decode(&reg); err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		if err := validate(&reg); err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		if _, dup := snap.regs[reg.Prefix]; dup {
			return nil, fmt.Errorf("%s: duplicate prefix %s", e.Name(), reg.Prefix)
		}
		snap.regs[reg.Prefix] = &reg
		for _, ch := range reg.Channels {
			if _, dup := snap.channels[ch.Name]; dup {
				return nil, fmt.Errorf("%s: duplicate channel %s", e.Name(), ch.Name)
			}
			snap.channels[ch.Name] = ch.Rule
		}
	}
	return snap, nil
}

func validate(r *Registration) error {
	if !prefixRe.MatchString(r.Prefix) {
		return fmt.Errorf("prefix %q must match %s", r.Prefix, prefixRe)
	}
	if reservedPrefixes[r.Prefix] {
		return fmt.Errorf("prefix %q is reserved", r.Prefix)
	}
	if r.Upstream == "" {
		return fmt.Errorf("upstream is required")
	}
	if !strings.Contains(r.Upstream, "://") && !strings.HasPrefix(r.Upstream, "dns:///") {
		r.Upstream = "dns:///" + r.Upstream
	}
	if r.DefaultRule == "" {
		r.DefaultRule = "authenticated"
	}
	if !ValidRule(r.DefaultRule) {
		return fmt.Errorf("invalid defaultRule %q", r.DefaultRule)
	}
	r.routeIdx = make(map[string]Route, len(r.Routes))
	for _, rt := range r.Routes {
		if rt.Method == "" || !strings.Contains(rt.Method, "/") {
			return fmt.Errorf("route method %q must be pkg.Service/Method", rt.Method)
		}
		if !ValidRule(rt.Rule) {
			return fmt.Errorf("route %s: invalid rule %q", rt.Method, rt.Rule)
		}
		if _, dup := r.routeIdx[rt.Method]; dup {
			return fmt.Errorf("duplicate route %s", rt.Method)
		}
		r.routeIdx[rt.Method] = rt
	}
	for _, ch := range r.Channels {
		if !channelRe.MatchString(ch.Name) {
			return fmt.Errorf("channel %q must match <product>.<topic>", ch.Name)
		}
		if !ValidRule(ch.Rule) {
			return fmt.Errorf("channel %s: invalid rule %q", ch.Name, ch.Rule)
		}
	}
	return nil
}

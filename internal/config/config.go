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
	pathRe           = regexp.MustCompile(`^(/[a-z0-9-]+)+$`)
	validVerbs       = map[string]bool{"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true}
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
	Verb     string   `yaml:"verb"`
	Path     string   `yaml:"path"`
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
}

func ValidRule(rule string) bool {
	return rule == "anonymous" || rule == "authenticated" || roleRe.MatchString(rule)
}

type routeKey struct {
	verb string
	path string // full path: prefix + Route.Path
}

// RouteMatch is a resolved public route: everything the handler needs to
// authorize and transcode one request, with no further config lookups.
type RouteMatch struct {
	Prefix     string        // owning registration prefix (for backend lookup)
	GRPCMethod string        // "pkg.Service/Method"
	Rule       string        // auth rule
	Deadline   time.Duration // upstream deadline (DefaultDeadline if unset)
	Metric     string        // bounded metric label (the full path)
}

type Snapshot struct {
	regs      map[string]*Registration // key: prefix
	channels  map[string]string        // channel name -> rule
	routes    map[routeKey]RouteMatch  // key: {verb, full path}
	pathVerbs map[string][]string      // full path -> sorted verbs (for 405/Allow)
}

// Route resolves an exact (HTTP method, full request path) to its route.
func (s *Snapshot) Route(verb, fullPath string) (RouteMatch, bool) {
	m, ok := s.routes[routeKey{verb: verb, path: fullPath}]
	return m, ok
}

// PathVerbs returns the verbs registered for a full path, sorted. Empty when
// the path is unknown — the handler uses this to pick 404 vs 405.
func (s *Snapshot) PathVerbs(fullPath string) []string { return s.pathVerbs[fullPath] }

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
	snap := &Snapshot{
		regs:      map[string]*Registration{},
		channels:  map[string]string{},
		routes:    map[routeKey]RouteMatch{},
		pathVerbs: map[string][]string{},
	}
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
		for _, rt := range reg.Routes {
			full := reg.Prefix + rt.Path
			key := routeKey{verb: rt.Verb, path: full}
			if _, dup := snap.routes[key]; dup {
				return nil, fmt.Errorf("%s: duplicate route %s %s", e.Name(), rt.Verb, full)
			}
			d := time.Duration(rt.Deadline)
			if d == 0 {
				d = DefaultDeadline
			}
			snap.routes[key] = RouteMatch{
				Prefix:     reg.Prefix,
				GRPCMethod: rt.Method,
				Rule:       rt.Rule,
				Deadline:   d,
				Metric:     full,
			}
			snap.pathVerbs[full] = append(snap.pathVerbs[full], rt.Verb)
		}
		for _, ch := range reg.Channels {
			if _, dup := snap.channels[ch.Name]; dup {
				return nil, fmt.Errorf("%s: duplicate channel %s", e.Name(), ch.Name)
			}
			snap.channels[ch.Name] = ch.Rule
		}
	}
	for p := range snap.pathVerbs {
		sort.Strings(snap.pathVerbs[p])
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
	for i := range r.Routes {
		rt := &r.Routes[i]
		if rt.Method == "" || !strings.Contains(rt.Method, "/") {
			return fmt.Errorf("route method %q must be pkg.Service/Method", rt.Method)
		}
		rt.Verb = strings.ToUpper(rt.Verb)
		if !validVerbs[rt.Verb] {
			return fmt.Errorf("route %s: verb %q must be one of GET POST PUT PATCH DELETE", rt.Method, rt.Verb)
		}
		if !pathRe.MatchString(rt.Path) {
			return fmt.Errorf("route %s: path %q must match %s", rt.Method, rt.Path, pathRe)
		}
		// An omitted rule inherits the registration defaultRule (already
		// defaulted and validated above); an explicit rule must be valid.
		if rt.Rule == "" {
			rt.Rule = r.DefaultRule
		} else if !ValidRule(rt.Rule) {
			return fmt.Errorf("route %s: invalid rule %q", rt.Method, rt.Rule)
		}
	}
	for i := range r.Channels {
		ch := &r.Channels[i]
		if !channelRe.MatchString(ch.Name) {
			return fmt.Errorf("channel %q must match <product>.<topic>", ch.Name)
		}
		if ch.Rule == "" {
			ch.Rule = r.DefaultRule
		} else if !ValidRule(ch.Rule) {
			return fmt.Errorf("channel %s: invalid rule %q", ch.Name, ch.Rule)
		}
	}
	return nil
}

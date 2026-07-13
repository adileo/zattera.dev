package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// dryRunMDKey signals a validate-only Apply (there is no ApplyRequest message;
// the flag travels in metadata).
const dryRunMDKey = "x-zattera-dry-run"

const stateChunkSize = 64 << 10

// documentVersion is the export schema version.
const documentVersion = 1

// StateServer implements zatterav1.StateServiceServer: a human-readable YAML
// export of DESIRED state and an idempotent, name-keyed apply.
type StateServer struct {
	zatterav1.UnimplementedStateServiceServer
	store *state.Store
	raft  Applier
	clock clock.Clock
}

// NewStateServer builds the state export/apply service.
func NewStateServer(store *state.Store, raft Applier, clk clock.Clock) *StateServer {
	return &StateServer{store: store, raft: raft, clock: clk}
}

// --- YAML document (desired state only; ids/meta excluded — names are the
// identity, so apply is idempotent and re-imports into the same cluster) ---

type document struct {
	Version  int          `yaml:"version"`
	Note     string       `yaml:"note,omitempty"`
	Projects []projectDoc `yaml:"projects,omitempty"`
}

type projectDoc struct {
	Name    string      `yaml:"name"`
	Apps    []appDoc    `yaml:"apps,omitempty"`
	Domains []domainDoc `yaml:"domains,omitempty"`
	Volumes []volumeDoc `yaml:"volumes,omitempty"`
}

type appDoc struct {
	Name         string         `yaml:"name"`
	Build        map[string]any `yaml:"build,omitempty"`
	GitHub       map[string]any `yaml:"github,omitempty"`
	Environments []envDoc       `yaml:"environments,omitempty"`
}

type envDoc struct {
	Name    string            `yaml:"name"`
	Type    string            `yaml:"type,omitempty"`
	Service map[string]any    `yaml:"service,omitempty"`
	EnvVars map[string]string `yaml:"env_vars,omitempty"` // key → base64(EncryptedValue proto)
}

type domainDoc struct {
	Hostname string         `yaml:"hostname"`
	App      string         `yaml:"app,omitempty"`
	Env      string         `yaml:"env,omitempty"`
	Fields   map[string]any `yaml:"fields,omitempty"`
}

type volumeDoc struct {
	Name   string         `yaml:"name"`
	App    string         `yaml:"app,omitempty"`
	Env    string         `yaml:"env,omitempty"`
	Fields map[string]any `yaml:"fields,omitempty"`
}

// Export streams the YAML document for a project (or the whole cluster).
func (s *StateServer) Export(req *zatterav1.ExportRequest, stream grpc.ServerStreamingServer[zatterav1.StateChunk]) error {
	doc, err := s.buildDocument(req.GetProjectId())
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	buf.WriteString("# Zattera desired-state export. Sealed env values re-import only\n")
	buf.WriteString("# into the SAME cluster (matching data key). Ids are not exported.\n")
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return status.Errorf(codes.Internal, "encode: %v", err)
	}
	_ = enc.Close()

	data := buf.Bytes()
	for len(data) > 0 {
		n := min(len(data), stateChunkSize)
		if err := stream.Send(&zatterav1.StateChunk{Data: data[:n]}); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (s *StateServer) buildDocument(projectID string) (*document, error) {
	doc := &document{Version: documentVersion}
	var projects []*zatterav1.Project
	if projectID != "" {
		p, ok := s.store.Project(projectID)
		if !ok {
			p, ok = s.store.ProjectByName(projectID)
		}
		if !ok {
			return nil, status.Errorf(codes.NotFound, "project %q not found", projectID)
		}
		projects = []*zatterav1.Project{p}
	} else {
		projects = s.store.ListProjects()
	}

	for _, p := range projects {
		pd := projectDoc{Name: p.GetName()}
		for _, app := range s.store.ListApps(p.GetMeta().GetId()) {
			ad := appDoc{Name: app.GetName()}
			if app.GetBuild() != nil {
				if m, err := toMap(app.GetBuild()); err == nil {
					ad.Build = m
				}
			}
			if app.GetGithub() != nil {
				if m, err := toMap(app.GetGithub()); err == nil {
					ad.GitHub = m
				}
			}
			for _, env := range s.store.ListEnvironments(p.GetMeta().GetId(), app.GetMeta().GetId()) {
				ed := envDoc{Name: env.GetName(), Type: env.GetType().String()}
				if env.GetService() != nil {
					if m, err := toMap(env.GetService()); err == nil {
						ed.Service = m
					}
				}
				vars := s.store.EnvVars(env.GetMeta().GetId())
				if len(vars) > 0 {
					ed.EnvVars = map[string]string{}
					for k, v := range vars {
						b, err := proto.Marshal(v)
						if err != nil {
							return nil, status.Errorf(codes.Internal, "marshal env var: %v", err)
						}
						ed.EnvVars[k] = base64.StdEncoding.EncodeToString(b)
					}
				}
				ad.Environments = append(ad.Environments, ed)
			}
			pd.Apps = append(pd.Apps, ad)
		}
		for _, d := range s.store.ListDomains(p.GetMeta().GetId()) {
			dd := domainDoc{Hostname: d.GetHostname()}
			dd.App, dd.Env = s.appEnvNames(d.GetAppId(), d.GetEnvironmentId())
			if m, err := toMap(domainExportView(d)); err == nil {
				dd.Fields = m
			}
			pd.Domains = append(pd.Domains, dd)
		}
		for _, v := range s.store.ListVolumes(p.GetMeta().GetId()) {
			vd := volumeDoc{Name: v.GetName()}
			vd.App, vd.Env = s.appEnvNames("", v.GetEnvironmentId())
			if m, err := toMap(volumeExportView(v)); err == nil {
				vd.Fields = m
			}
			pd.Volumes = append(pd.Volumes, vd)
		}
		doc.Projects = append(doc.Projects, pd)
	}
	return doc, nil
}

func (s *StateServer) appEnvNames(appID, envID string) (app, env string) {
	if a, ok := s.store.App(appID); ok {
		app = a.GetName()
	}
	if e, ok := s.store.Environment(envID); ok {
		env = e.GetName()
		if app == "" {
			if a, ok := s.store.App(e.GetAppId()); ok {
				app = a.GetName()
			}
		}
	}
	return app, env
}

// Apply reassembles the streamed YAML, diffs by name, and upserts. dry-run
// (metadata) validates and counts without writing.
func (s *StateServer) Apply(stream grpc.ClientStreamingServer[zatterav1.StateChunk, zatterav1.ApplyResponse]) error {
	var buf bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		buf.Write(chunk.GetData())
	}
	var doc document
	if err := yaml.Unmarshal(buf.Bytes(), &doc); err != nil {
		return status.Errorf(codes.InvalidArgument, "parse export: %v", err)
	}
	dryRun := metadataFlag(stream.Context(), dryRunMDKey)

	resp := &zatterav1.ApplyResponse{}
	if err := s.applyDocument(stream.Context(), &doc, dryRun, resp); err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

func (s *StateServer) applyDocument(ctx context.Context, doc *document, dryRun bool, resp *zatterav1.ApplyResponse) error {
	for _, pd := range doc.Projects {
		proj, changed, err := s.upsertProject(ctx, pd.Name, dryRun)
		if err != nil {
			return err
		}
		count(resp, changed)

		for _, ad := range pd.Apps {
			app, changed, err := s.upsertApp(ctx, proj, ad, dryRun, resp)
			if err != nil {
				return err
			}
			count(resp, changed)
			for _, ed := range ad.Environments {
				changed, err := s.upsertEnv(ctx, proj, app, ed, dryRun, resp)
				if err != nil {
					return err
				}
				count(resp, changed)
			}
		}

		for _, dd := range pd.Domains {
			changed, err := s.upsertDomain(ctx, proj, dd, dryRun, resp)
			if err != nil {
				return err
			}
			count(resp, changed)
		}
		for _, vd := range pd.Volumes {
			changed, err := s.upsertVolume(ctx, proj, vd, dryRun, resp)
			if err != nil {
				return err
			}
			count(resp, changed)
		}
	}
	return nil
}

// resolveEnvByName resolves (app name, env name) within a project to the app
// and environment. Both may be empty for a project-scoped resource.
func (s *StateServer) resolveEnvByName(projectID, appName, envName string) (appID, envID string) {
	if appName == "" {
		return "", ""
	}
	app, ok := s.store.AppByName(projectID, appName)
	if !ok {
		return "", ""
	}
	appID = app.GetMeta().GetId()
	if envName != "" {
		if env, ok := s.store.EnvironmentByName(appID, envName); ok {
			envID = env.GetMeta().GetId()
		}
	}
	return appID, envID
}

func (s *StateServer) upsertDomain(ctx context.Context, proj *zatterav1.Project, dd domainDoc, dryRun bool, resp *zatterav1.ApplyResponse) (changeKind, error) {
	desired := &zatterav1.Domain{}
	if dd.Fields != nil {
		if err := fromMap(dd.Fields, desired); err != nil {
			resp.Warnings = append(resp.Warnings, fmt.Sprintf("domain %s: %v", dd.Hostname, err))
		}
	}
	desired.Hostname = dd.Hostname
	desired.ProjectId = proj.GetMeta().GetId()
	desired.AppId, desired.EnvironmentId = s.resolveEnvByName(proj.GetMeta().GetId(), dd.App, dd.Env)

	existing, ok := s.store.DomainByHostname(dd.Hostname)
	if !ok {
		desired.Meta = s.newMeta()
		if !dryRun {
			if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutDomain{PutDomain: &clusterv1.PutDomain{Domain: desired}}}); err != nil {
				return unchanged, toStatus(err)
			}
		}
		return created, nil
	}
	merged := clone(existing)
	proto.Merge(merged, desired) // overlay desired onto existing, preserving observed fields
	merged.Meta = existing.GetMeta()
	if proto.Equal(existing, merged) {
		return unchanged, nil
	}
	merged.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
	if !dryRun {
		if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutDomain{PutDomain: &clusterv1.PutDomain{Domain: merged}}}); err != nil {
			return unchanged, toStatus(err)
		}
	}
	return updated, nil
}

func (s *StateServer) upsertVolume(ctx context.Context, proj *zatterav1.Project, vd volumeDoc, dryRun bool, resp *zatterav1.ApplyResponse) (changeKind, error) {
	desired := &zatterav1.Volume{}
	if vd.Fields != nil {
		if err := fromMap(vd.Fields, desired); err != nil {
			resp.Warnings = append(resp.Warnings, fmt.Sprintf("volume %s: %v", vd.Name, err))
		}
	}
	desired.Name = vd.Name
	desired.ProjectId = proj.GetMeta().GetId()
	_, desired.EnvironmentId = s.resolveEnvByName(proj.GetMeta().GetId(), vd.App, vd.Env)

	existing, ok := s.store.VolumeByName(proj.GetMeta().GetId(), desired.GetEnvironmentId(), vd.Name)
	if !ok {
		desired.Meta = s.newMeta()
		if !dryRun {
			if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutVolume{PutVolume: &clusterv1.PutVolume{Volume: desired}}}); err != nil {
				return unchanged, toStatus(err)
			}
		}
		return created, nil
	}
	merged := clone(existing)
	proto.Merge(merged, desired)
	merged.Meta = existing.GetMeta()
	if proto.Equal(existing, merged) {
		return unchanged, nil
	}
	merged.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
	if !dryRun {
		if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutVolume{PutVolume: &clusterv1.PutVolume{Volume: merged}}}); err != nil {
			return unchanged, toStatus(err)
		}
	}
	return updated, nil
}

// changeKind classifies an upsert outcome.
type changeKind int

const (
	unchanged changeKind = iota
	created
	updated
)

func count(resp *zatterav1.ApplyResponse, k changeKind) {
	switch k {
	case created:
		resp.Created++
	case updated:
		resp.Updated++
	default:
		resp.Unchanged++
	}
}

func (s *StateServer) upsertProject(ctx context.Context, name string, dryRun bool) (*zatterav1.Project, changeKind, error) {
	if p, ok := s.store.ProjectByName(name); ok {
		return p, unchanged, nil
	}
	p := &zatterav1.Project{Meta: s.newMeta(), Name: name}
	if !dryRun {
		if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutProject{PutProject: &clusterv1.PutProject{Project: p}}}); err != nil {
			return nil, unchanged, toStatus(err)
		}
	}
	return p, created, nil
}

func (s *StateServer) upsertApp(ctx context.Context, proj *zatterav1.Project, ad appDoc, dryRun bool, resp *zatterav1.ApplyResponse) (*zatterav1.App, changeKind, error) {
	build := &zatterav1.BuildConfig{}
	if ad.Build != nil {
		if err := fromMap(ad.Build, build); err != nil {
			resp.Warnings = append(resp.Warnings, fmt.Sprintf("app %s: build: %v", ad.Name, err))
		}
	}
	var gh *zatterav1.GitHubConfig
	if ad.GitHub != nil {
		gh = &zatterav1.GitHubConfig{}
		if err := fromMap(ad.GitHub, gh); err != nil {
			resp.Warnings = append(resp.Warnings, fmt.Sprintf("app %s: github: %v", ad.Name, err))
		}
	}

	existing, ok := s.store.AppByName(proj.GetMeta().GetId(), ad.Name)
	if !ok {
		app := &zatterav1.App{Meta: s.newMeta(), ProjectId: proj.GetMeta().GetId(), Name: ad.Name, Build: build, Github: gh}
		if !dryRun {
			if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutApp{PutApp: &clusterv1.PutApp{App: app}}}); err != nil {
				return nil, unchanged, toStatus(err)
			}
		}
		return app, created, nil
	}
	// Compare at the normalized protojson-map level (see sameProto) so YAML/JSON
	// type drift never causes a spurious update.
	if sameProto(existing.GetBuild(), build) && sameProto(existing.GetGithub(), gh) {
		return existing, unchanged, nil
	}
	desired := clone(existing)
	desired.Build = build
	desired.Github = gh
	desired.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
	if !dryRun {
		if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutApp{PutApp: &clusterv1.PutApp{App: desired}}}); err != nil {
			return nil, unchanged, toStatus(err)
		}
	}
	return desired, updated, nil
}

func (s *StateServer) upsertEnv(ctx context.Context, proj *zatterav1.Project, app *zatterav1.App, ed envDoc, dryRun bool, resp *zatterav1.ApplyResponse) (changeKind, error) {
	spec := &zatterav1.ServiceSpec{}
	if ed.Service != nil {
		if err := fromMap(ed.Service, spec); err != nil {
			resp.Warnings = append(resp.Warnings, fmt.Sprintf("env %s/%s: service: %v", app.GetName(), ed.Name, err))
		}
	}
	desiredVars, err := decodeEnvVars(ed.EnvVars)
	if err != nil {
		return unchanged, status.Errorf(codes.InvalidArgument, "env %s/%s: %v", app.GetName(), ed.Name, err)
	}

	existing, ok := s.store.EnvironmentByName(app.GetMeta().GetId(), ed.Name)
	envKind := unchanged
	var envID string
	if !ok {
		env := &zatterav1.Environment{
			Meta: s.newMeta(), AppId: app.GetMeta().GetId(), ProjectId: proj.GetMeta().GetId(),
			Name: ed.Name, Type: envType(ed.Type), Service: spec,
		}
		envID = env.GetMeta().GetId()
		envKind = created
		if !dryRun {
			if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: env}}}); err != nil {
				return unchanged, toStatus(err)
			}
		}
	} else {
		envID = existing.GetMeta().GetId()
		specSame := sameProto(existing.GetService(), spec)
		typeSame := envType(ed.Type) == zatterav1.EnvironmentType_ENVIRONMENT_TYPE_UNSPECIFIED || existing.GetType() == envType(ed.Type)
		if !specSame || !typeSame {
			envKind = updated
			desired := clone(existing)
			desired.Service = spec
			if t := envType(ed.Type); t != zatterav1.EnvironmentType_ENVIRONMENT_TYPE_UNSPECIFIED {
				desired.Type = t
			}
			desired.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
			if !dryRun {
				if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: desired}}}); err != nil {
					return unchanged, toStatus(err)
				}
			}
		}
	}

	// Env vars (already sealed; set directly).
	if varsChanged(s.store.EnvVars(envID), desiredVars) {
		if envKind == unchanged {
			envKind = updated
		}
		if !dryRun {
			unset := missingKeys(s.store.EnvVars(envID), desiredVars)
			if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_SetEnvVars{SetEnvVars: &clusterv1.SetEnvVars{
				EnvironmentId: envID, Set: desiredVars, Unset: unset,
			}}}); err != nil {
				return unchanged, toStatus(err)
			}
		}
	}
	return envKind, nil
}

// --- helpers ---

func (s *StateServer) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:state-apply"
	cmd.Time = timestamppb.Now()
	return s.raft.Apply(ctx, cmd)
}

func (s *StateServer) newMeta() *zatterav1.Meta {
	ts := timestamppb.New(s.clock.Now())
	return &zatterav1.Meta{Id: ids.New(), CreatedAt: ts, UpdatedAt: ts}
}

// sameProto reports whether two proto messages are equal at the normalized
// protojson-map level. Both operands go through toMap, so the comparison is
// immune to representation drift (YAML ints vs JSON float64, protojson field
// ordering) that a raw proto.Equal or map-vs-yaml compare would trip on. This
// is what makes apply an exact idempotent fixpoint. The `reconstructed` side is
// typically fromMap(docMap), so comparing its toMap form to the existing
// message's toMap form cancels any round-trip noise.
func sameProto(existing, reconstructed proto.Message) bool {
	return reflect.DeepEqual(mapOrNil(existing), mapOrNil(reconstructed))
}

func mapOrNil(m proto.Message) map[string]any {
	if m == nil || reflect.ValueOf(m).IsNil() {
		return nil
	}
	x, err := toMap(m)
	if err != nil {
		return nil
	}
	return x
}

func toMap(m proto.Message) (map[string]any, error) {
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func fromMap(in map[string]any, m proto.Message) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return protojson.Unmarshal(b, m)
}

func decodeEnvVars(in map[string]string) (map[string]*zatterav1.EncryptedValue, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(map[string]*zatterav1.EncryptedValue, len(in))
	for k, b64 := range in {
		raw, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("env var %s: bad base64", k)
		}
		var ev zatterav1.EncryptedValue
		if err := proto.Unmarshal(raw, &ev); err != nil {
			return nil, fmt.Errorf("env var %s: %w", k, err)
		}
		out[k] = &ev
	}
	return out, nil
}

func varsChanged(existing, desired map[string]*zatterav1.EncryptedValue) bool {
	if len(existing) != len(desired) {
		return true
	}
	for k, v := range desired {
		ev, ok := existing[k]
		if !ok || !proto.Equal(ev, v) {
			return true
		}
	}
	return false
}

func missingKeys(existing, desired map[string]*zatterav1.EncryptedValue) []string {
	var out []string
	for k := range existing {
		if _, ok := desired[k]; !ok {
			out = append(out, k)
		}
	}
	return out
}

func envType(s string) zatterav1.EnvironmentType {
	if v, ok := zatterav1.EnvironmentType_value[s]; ok {
		return zatterav1.EnvironmentType(v)
	}
	return zatterav1.EnvironmentType_ENVIRONMENT_TYPE_UNSPECIFIED
}

func metadataFlag(ctx context.Context, key string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	vals := md.Get(key)
	return len(vals) > 0 && (vals[0] == "1" || vals[0] == "true")
}

// domainExportView strips ids/meta/observed state, keeping desired config.
func domainExportView(d *zatterav1.Domain) *zatterav1.Domain {
	return &zatterav1.Domain{
		Hostname: d.GetHostname(), PathPrefix: d.GetPathPrefix(),
		Middleware: d.GetMiddleware(), ClusterSubdomain: d.GetClusterSubdomain(), PortName: d.GetPortName(),
	}
}

func volumeExportView(v *zatterav1.Volume) *zatterav1.Volume {
	return &zatterav1.Volume{
		Name: v.GetName(), SnapshotPolicy: v.GetSnapshotPolicy(),
	}
}

package scheduler

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// reconcileStateful drives a stateful deployment through stop-then-start (T-63,
// spec §9.2). Unlike red/green, the old instance is fully stopped before the new
// one starts on the SAME node and volume — there is never a moment with two RUN
// assignments for the volume. A brief maintenance downtime is expected and
// signalled with events. A failure after the old instance has stopped restarts
// it (best effort) before failing.
//
//	PENDING → [BUILDING] → STOPPING_OLD → STARTING → HEALTHCHECKING
//	        → PROMOTING → SUCCEEDED
func (o *Orchestrator) reconcileStateful(ctx context.Context, st *state.Store, env *zatterav1.Environment, rel *zatterav1.Release, d *zatterav1.Deployment) error {
	switch d.GetPhase() {
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING:
		if d.GetBuildId() != "" && rel.GetImageRef() == "" {
			return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_BUILDING, "")
		}
		if rel.GetImageRef() == "" {
			return o.abort(ctx, d, "release has no image ref")
		}
		return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STOPPING_OLD, "")

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_BUILDING:
		return o.checkBuild(ctx, rel, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STOPPING_OLD)

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STOPPING_OLD:
		return o.stopOld(ctx, st, d)

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING:
		return o.statefulStart(ctx, st, env, rel, d)

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING:
		return o.statefulHealth(ctx, st, rel, d)

	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING:
		return o.statefulPromote(ctx, d)
	}
	return nil
}

// stopOld flips the outgoing instance to STOP and waits for it to actually stop
// (its container gone) before the new one may start — the single-writer barrier.
func (o *Orchestrator) stopOld(ctx context.Context, st *state.Store, d *zatterav1.Deployment) error {
	old := oldInstances(st, d)

	var stopPuts []*zatterav1.Assignment
	for _, a := range old {
		if a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			a.Desired = zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_STOP
			stopPuts = append(stopPuts, a)
		}
	}
	if len(stopPuts) > 0 {
		if err := o.apply(ctx, putAssignments(stopPuts)); err != nil {
			return err
		}
		o.emitEvent(ctx, d, "deploy.maintenance_start", "warning",
			"stateful update: stopping the current instance (brief downtime)")
		return nil // wait for the agent to report STOPPED
	}
	// All old instances are flipped to STOP; wait until none still has a
	// container running before starting the new one.
	if stillRunning(old) {
		return nil
	}
	return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING, "")
}

// statefulStart places exactly one new instance on the volume's node (Place
// pins it) and waits for it to run. A failure here restarts the old instance.
func (o *Orchestrator) statefulStart(ctx context.Context, st *state.Store, env *zatterav1.Environment, rel *zatterav1.Release, d *zatterav1.Deployment) error {
	green := greenAssignments(st, d)
	if len(green) == 0 {
		// Place pins to the volume's node when it exists; on a first deploy the
		// volume does not exist yet, so it picks the least-loaded node and we pin
		// the new volume there before placing.
		picks, err := Place(st, rel, env.GetMeta().GetId(), 1, nil)
		if len(picks) == 0 {
			return o.statefulFail(ctx, st, d, "no node available for the stateful instance: "+errString(err))
		}
		node := picks[0]
		if err := o.ensureDeployVolumes(ctx, st, env, rel, node); err != nil {
			return err
		}
		put := greenAssignment(env, rel, node, d.GetMeta().GetId())
		return o.apply(ctx, putAssignments([]*zatterav1.Assignment{put}))
	}
	if anyFailed(green) {
		return o.statefulFail(ctx, st, d, "the new instance failed to start")
	}
	for _, a := range green {
		s := a.GetObserved().GetState()
		if s != zatterav1.InstanceState_INSTANCE_STATE_RUNNING && s != zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
			return nil // still coming up (or waiting for its volume lease)
		}
	}
	return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING, "")
}

// statefulHealth waits for the new instance to become HEALTHY within the
// deadline; a failure or timeout restarts the old instance.
func (o *Orchestrator) statefulHealth(ctx context.Context, st *state.Store, rel *zatterav1.Release, d *zatterav1.Deployment) error {
	green := greenAssignments(st, d)
	if anyFailed(green) {
		return o.statefulFail(ctx, st, d, "the new instance became unhealthy")
	}
	healthy := len(green) > 0
	for _, a := range green {
		if a.GetObserved().GetState() != zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
			healthy = false
			break
		}
	}
	if healthy {
		return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING, "")
	}
	entry := d.GetMeta().GetUpdatedAt().AsTime()
	if o.clock.Now().After(entry.Add(healthDeadline(rel))) {
		return o.statefulFail(ctx, st, d, "the new instance did not become healthy in time")
	}
	return nil
}

// statefulPromote switches the active release to the new one and completes.
// There is no drain window — the old instance is already stopped.
func (o *Orchestrator) statefulPromote(ctx context.Context, d *zatterav1.Deployment) error {
	if err := o.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PromoteRelease{PromoteRelease: &clusterv1.PromoteRelease{
		EnvironmentId: d.GetEnvironmentId(),
		ReleaseId:     d.GetReleaseId(),
		DeploymentId:  d.GetMeta().GetId(),
	}}}); err != nil {
		return err
	}
	o.emitEvent(ctx, d, "deploy.maintenance_end", "info", "stateful update: new instance healthy and promoted")
	return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED, "")
}

// statefulFail reaps the new instance, restarts the old one (best effort, to
// minimize downtime), and fails the deployment.
func (o *Orchestrator) statefulFail(ctx context.Context, st *state.Store, d *zatterav1.Deployment, reason string) error {
	if err := o.reapGreen(ctx, st, d); err != nil {
		return err
	}
	// Best effort: flip the old instance back to RUN so the scheduler + agent
	// bring it up again (its volume lease follows it once green is gone).
	var restart []*zatterav1.Assignment
	for _, a := range oldInstances(st, d) {
		if a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			a.Desired = zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN
			restart = append(restart, a)
		}
	}
	if len(restart) > 0 {
		if err := o.apply(ctx, putAssignments(restart)); err != nil {
			return err
		}
		o.emitEvent(ctx, d, "deploy.rolled_back", "warning", "stateful update failed; restarted the previous instance")
	}
	o.emitEvent(ctx, d, "deploy.failed", "error", reason)
	return o.setPhase(ctx, d, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED, reason)
}

// ensureDeployVolumes creates any volume the release declares that does not yet
// exist, pinning it to node (used on a first stateful deploy, where the
// scheduler has not auto-created it because the env has no active release yet).
func (o *Orchestrator) ensureDeployVolumes(ctx context.Context, st *state.Store, env *zatterav1.Environment, rel *zatterav1.Release, node string) error {
	for _, vm := range rel.GetService().GetVolumes() {
		if _, ok := st.VolumeByName(env.GetProjectId(), env.GetMeta().GetId(), vm.GetVolumeName()); ok {
			continue
		}
		now := timestamppb.New(o.clock.Now())
		v := &zatterav1.Volume{
			Meta:          &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now},
			ProjectId:     env.GetProjectId(),
			EnvironmentId: env.GetMeta().GetId(),
			Name:          vm.GetVolumeName(),
			NodeId:        node,
			Status:        zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		}
		if err := o.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutVolume{PutVolume: &clusterv1.PutVolume{Volume: v}}}); err != nil {
			return err
		}
	}
	return nil
}

// oldInstances returns the deployment's outgoing (previous-release) instances
// regardless of their desired state — so a STOP'd-but-not-yet-STOPPED instance
// is still tracked while we wait for it to actually stop.
func oldInstances(st *state.Store, d *zatterav1.Deployment) []*zatterav1.Assignment {
	prev := d.GetPreviousReleaseId()
	if prev == "" {
		return nil
	}
	var out []*zatterav1.Assignment
	for _, a := range st.ListAssignments(d.GetEnvironmentId()) {
		if a.GetReleaseId() == prev && a.GetDeploymentId() != d.GetMeta().GetId() {
			out = append(out, a)
		}
	}
	return out
}

// stillRunning reports whether any instance still has (or is expected to have) a
// live container — anything that has not yet reached STOPPED/FAILED.
func stillRunning(as []*zatterav1.Assignment) bool {
	for _, a := range as {
		switch a.GetObserved().GetState() {
		case zatterav1.InstanceState_INSTANCE_STATE_STOPPED,
			zatterav1.InstanceState_INSTANCE_STATE_FAILED,
			zatterav1.InstanceState_INSTANCE_STATE_UNSPECIFIED:
			// gone (or never started)
		default:
			return true
		}
	}
	return false
}

func errString(err error) string {
	if err == nil {
		return "unknown"
	}
	return err.Error()
}

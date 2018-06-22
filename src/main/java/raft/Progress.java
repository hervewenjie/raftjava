package raft;

import lombok.Builder;
import util.Panic;

import static raft.ProgressStateType.ProgressStateReplicate;
import static raft.ProgressStateType.ProgressStateSnapshot;

/**
 * Progress represents a follower’s progress in the view of the leader. Leader maintains
 * progresses of all followers, and sends entries to the follower based on its progress
 *
 * Created by chengwenjie on 2018/5/30.
 */
@Builder
public class Progress {

    public long Match, Next;

    // State defines how the leader should interact with the follower.
    //
    // When in ProgressStateProbe, leader sends at most one replication message
    // per heartbeat interval. It also probes actual progress of the follower.
    //
    // When in ProgressStateReplicate, leader optimistically increases next
    // to the latest entry sent after sending replication message. This is
    // an optimized state for fast replicating log entries to the follower.
    //
    // When in ProgressStateSnapshot, leader should have sent out snapshot
    // before and stops sending any replication message.
    public ProgressStateType State;

    // Paused is used in ProgressStateProbe.
    // When Paused is true, raft should pause sending replication message to this peer.
    public boolean Paused;

    // PendingSnapshot is used in ProgressStateSnapshot.
    // If there is a pending snapshot, the pendingSnapshot will be set to the
    // index of the snapshot. If pendingSnapshot is set, the replication process of
    // this Progress will be paused. raft will not resend snapshot until the pending one
    // is reported to be failed.
    public long PendingSnapshot;

    // RecentActive is true if the progress is recently active. Receiving any messages
    // from the corresponding follower indicates the progress is active.
    // RecentActive can be reset to false after an election timeout.
    public boolean RecentActive;

    // inflights is a sliding window for the inflight messages.
    // Each inflight message contains one or more log entries.
    // The max number of entries per message is defined in raft config as MaxSizePerMsg.
    // Thus inflight effectively limits both the number of inflight messages
    // and the bandwidth each Progress can use.
    // When inflights is full, no more message should be sent.
    // When a leader sends out a message, the index of the last
    // entry should be added to inflights. The index MUST be added
    // into inflights in order.
    // When a leader receives a reply, the previous inflights should
    // be freed by calling inflights.freeTo with the index of the last
    // received entry.
    public Inflights ins;

    // IsLearner is true if this progress is tracked for a learner.
    public boolean isLearner;

    // IsPaused returns whether sending log entries to this node has been
    // paused. A node may be paused because it has rejected recent
    // MsgApps, is currently waiting for a snapshot, or has reached the
    // MaxInflightMsgs limit.
    public boolean IsPaused() {
        if (State == null) return false;
        switch (State) {
            case ProgressStateProbe:
                return Paused;
            case ProgressStateReplicate:
                return ins.full();
            case ProgressStateSnapshot:
                return true;
            default:
                Panic.panic("unexpected state");
        }
        return false;
    }

    public void optimisticUpdate(long n) {
        this.Next = n + 1;
    }

    public void pause() {Paused = true;}
    public void resume() {Paused = false;}

    public void becomeProbe() {

    }

    public void becomeSnapshot(long snapshoti) {
        resetState(ProgressStateSnapshot);
        PendingSnapshot = snapshoti;
    }

    // maybeUpdate returns false if the given n index comes from an outdated message.
    // Otherwise it updates the progress and returns true.
    public boolean maybeUpdate(long n) {
        boolean updated = false;
        if (Match < n) {
            Match = n;
            updated = true;
            this.resume();
        }
        if (Next < n + 1) {
            Next = n + 1;
        }
        return updated;
    }

    public void becomeReplicate() {
        this.resetState(ProgressStateReplicate);
        this.Next = this.Match + 1;
    }

    // needSnapshotAbort returns true if snapshot progress's Match
    // is equal or higher than the pendingSnapshot.
    public boolean needSnapshotAbort() {
        return State == ProgressStateSnapshot && Match >= PendingSnapshot;
    }

    // maybeDecrTo returns false if the given to index comes from an out of order message.
    // Otherwise it decreases the progress next index to min(rejected, last) and returns true.
    public boolean maybeDecrTo(long rejected, long last) {
        if (State == ProgressStateReplicate) {
            // the rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            if (rejected <= Match) {
                return false;
            }
            // directly decrease next to match + 1
            Next = Match + 1;
            return true;
        }

        // the rejection must be stale if "rejected" does not match next - 1
        if (Next - 1 != rejected) {
            return false;
        }

        Next = Math.min(rejected, last + 1);
        if (Next < 1) {
            Next = 1;
        }
        this.resume();
        return true;
    }

    // - - - - -
    // privates
    private void resetState(ProgressStateType state) {
        this.Paused = false;
        this.PendingSnapshot = 0L;
        this.State = state;
        this.ins.reset();
    }
}

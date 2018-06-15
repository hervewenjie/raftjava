package raft;

import lombok.Builder;
import pb.HardState;

/**
 * Ready encapsulates the entries and messages that are ready to read,
 * be saved to stable storage, committed or sent to other peers.
 * All fields in Ready are read-only.
 *
 * Created by chengwenjie on 2018/6/4.
 */
@Builder
public class Ready {

    // The current volatile state of a NodeImpl.
    // SoftState will be nil if there is no update.
    // It is not required to consume or store SoftState.
	public SoftState SoftState;

    // The current state of a NodeImpl to be saved to stable storage BEFORE
    // Messages are sent.
    // HardState will be equal to empty state if there is no update.
    public pb.HardState HardState;

    // ReadStates can be used for node to serve linearizable read requests locally
    // when its applied index is greater than the index in ReadState.
    // Note that the readState will be returned when raft receives msgReadIndex.
    // The returned is only valid for the request that requested to read.
    public ReadState[] ReadStates;

    // Entries specifies entries to be saved to stable storage BEFORE
    // Messages are sent.
    public pb.Entry[] Entries;

    // Snapshot specifies the snapshot to be saved to stable storage.
    public pb.Snapshot Snapshot;

    // CommittedEntries specifies entries to be committed to a
    // store/state-machine. These have previously been committed to stable
    // store.
    public pb.Entry[] CommittedEntries;

    // Messages specifies outbound messages to be sent AFTER Entries are
    // committed to stable storage.
    // If it contains a MsgSnap message, the application MUST report back to raft
    // when the snapshot has been received or has failed by calling ReportSnapshot.
    public pb.Message[] Messages;

    // MustSync indicates whether the HardState and Entries must be synchronously
    // written to disk or if an asynchronous write is permissible.
    public boolean MustSync;

    static HardState emptyState = new HardState();

    public boolean containsUpdates() {
        return SoftState != null || !IsEmptyHardState(HardState) || !IsEmptySnap(Snapshot) || (Entries != null && Entries.length > 0)
                || (CommittedEntries != null && CommittedEntries.length > 0)
                || (Messages != null && Messages.length > 0)
                || (ReadStates != null && ReadStates.length > 0);
    }

    public static boolean IsEmptyHardState(pb.HardState st) {
        if (st == null) return true;
        return isHardStateEqual(st, emptyState);
    }

    private static boolean isHardStateEqual(pb.HardState a, pb.HardState b) {
        return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit;
    }

    public static boolean IsEmptySnap(pb.Snapshot sp) {
        if (sp == null) return true;
        return sp.Metadata.Index == 0L;
    }

    // MustSync returns true if the hard state and count of Raft entries indicate
    // that a synchronous write to persistent storage is required.
    public static boolean MustSync(HardState st, HardState prevst, int entsnum) {
        // Persistent state on all servers:
        // (Updated on stable storage before responding to RPCs)
        // currentTerm
        // votedFor
        // log entries[]
        return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term;
    }
}

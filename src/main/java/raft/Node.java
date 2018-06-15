package raft;

import pb.SnapshotStatus;

import java.util.Queue;

/**
 * Created by chengwenjie on 2018/6/12.
 */
public interface Node {

    // Tick increments the internal logical clock for the Node by a single tick. Election
    // timeouts and heartbeat timeouts are in units of ticks.
    void Tick();

    // Campaign causes the Node to transition to candidate state and start campaigning to become leader.
    void Campaign(context.Context ctx);

    // Propose proposes that data be appended to the log.
    void Propose(context.Context ctx, byte[] data);

    // ProposeConfChange proposes config change.
    // At most one ConfChange can be in the process of going through consensus.
    // Application needs to call ApplyConfChange when applying EntryConfChange type entry.
    void ProposeConfChange(context.Context ctx, pb.ConfChange cc);

    // Step advances the state machine using the given message. ctx.Err() will be returned, if any.
    void Step(context.Context ctx, pb.Message msg);

    // Ready returns a channel that returns the current point-in-time state.
    // Users of the Node must call Advance after retrieving the state returned by Ready.
    //
    // NOTE: No committed entries from the next Ready may be applied until all committed entries
    // and snapshots from the previous one have finished.
    Queue<Ready> Ready();

    // Advance notifies the Node that the application has saved progress up to the last Ready.
    // It prepares the node to return the next available Ready.
    //
    // The application should generally call Advance after it applies the entries in last Ready.
    //
    // However, as an optimization, the application may call Advance while it is applying the
    // commands. For example. when the last Ready contains a snapshot, the application might take
    // a long time to apply the snapshot data. To continue receiving Ready without blocking raft
    // progress, it can call Advance before finishing applying the last ready.
    void Advance();

    // ApplyConfChange applies config change to the local node.
    // Returns an opaque ConfState protobuf which must be recorded
    // in snapshots. Will never return nil; it returns a pointer only
    // to match MemoryStorage.Compact.
    pb.ConfState ApplyConfChange(pb.ConfChange cc);

    // TransferLeadership attempts to transfer leadership to the given transferee.
    void TransferLeadership(context.Context ctx, long lead, long transferee);

    // ReadIndex request a read state. The read state will be set in the ready.
    // Read state has a read index. Once the application advances further than the read
    // index, any linearizable read requests issued before the read request can be
    // processed safely. The read state will have the same rctx attached.
    void ReadIndex(context.Context ctx, byte[] rctx);

    // Status returns the current status of the raft state machine.
    Status Status();

    // ReportUnreachable reports the given node is not reachable for the last send.
    void ReportUnreachable(long id);

    // ReportSnapshot reports the status of the sent snapshot.
    void ReportSnapshot(long id, SnapshotStatus status);

    // Stop performs any necessary termination of the Node.
    void Stop();
}

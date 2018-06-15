package raft;

import javafx.util.Pair;

/**
 * Storage is an interface that may be implemented by the application,
 * to retrieve log entries from storage.
 *
 * If any Storage method returns an error, the raft instance will
 * become inoperable and refuse to participate in elections; the
 * application is responsible for cleanup and recovery in this case.
 *
 * Created by chengwenjie on 2018/5/30.
 */
public interface Storage {

    // InitialState returns the saved HardState and ConfState information.
    Pair<pb.HardState, pb.ConfState> initialState();

    // Entries returns a slice of log entries in the range [lo,hi).
    // MaxSize limits the total size of the log entries returned, but
    // Entries returns at least one entry if any.
    pb.Entry[] entries(long lo, long hi, long maxSize);

    // Term returns the term of entry i, which must be in the range
    // [FirstIndex()-1, LastIndex()]. The term of the entry before
    // FirstIndex is retained for matching purposes even though the
    // rest of that entry may not be available.
    long term(Long i);

    // LastIndex returns the index of the last entry in the log.
    long LastIndex();

    // FirstIndex returns the index of the first log entry that is
    // possibly available via Entries (older entries have been incorporated
    // into the latest Snapshot; if storage only contains the dummy entry the
    // first log entry is not available).
    long FirstIndex();

    // Snapshot returns the most recent snapshot.
    // If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
    // so raft state machine could know that Storage needs some time to prepare
    // snapshot and call Snapshot later.
    pb.Snapshot Snapshot();
}

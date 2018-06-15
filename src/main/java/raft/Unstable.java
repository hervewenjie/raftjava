package raft;

import javafx.util.Pair;

import java.util.Arrays;

/**
 * unstable.entries[i] has raft log position i+unstable.offset.
 * Note that unstable.offset may be less than the highest log
 * position in storage; this means that the next write to storage
 * might need to truncate the log before persisting unstable.entries.
 *
 * Created by chengwenjie on 2018/5/30.
 */
public class Unstable {

    // the incoming unstable snapshot, if any.
    pb.Snapshot snapshot;
    // all entries that have not yet been written to storage.
    pb.Entry[] entries = {};
    long offset;

    Logger logger;

    Pair<Long, Boolean> maybeLastIndex() {
        int l;
        if ((l = entries.length) != 0) {
            return new Pair<>(this.offset + Long.valueOf(l) - 1, true);
        }
        if (snapshot != null) {
            return new Pair<>(snapshot.Metadata.Index, true);
        }
        return new Pair<>(0L, false);
    }

    Pair<Long, Boolean> maybeFirstIndex() {
        if (this.snapshot != null) {
            return new Pair<>(this.snapshot.Metadata.Index + 1, true);
        }
        return new Pair<>(0L, false);
    }

    //  maybeTerm returns the term of the entry at index i, if there is any
    long maybeTerm(Long i) {
        if (i < this.offset) {
            if (this.snapshot == null) {
                return -1L;
            }
            if (this.snapshot.Metadata.Index == i) {
                return snapshot.Metadata.Term;
            }
            return -1L;
        }
        long last = this.maybeLastIndex().getKey();
        if (i > last) {
            return -1L;
        }
        return entries[Long.valueOf(i - this.offset).intValue()].Term;
    }

    public void truncateAndAppend(pb.Entry... ents) {
        long after = ents[0].Index;
        if (after == offset + entries.length) {
            // after is the next index in the u.entries
            // directly append
        } else if (after <= offset) {
            logger.info(String.format("replace the unstable entries from index %d", after));
            // The log is being truncated to before our current offset
            // portion, so set the offset and replace the entries
            offset = after;
            entries = ents;
        } else {
            // truncate to after and copy to u.entries
            // then append
            logger.info(String.format("truncate the unstable entries before index %d", after));
            // TODO entris append
        }
    }

    public void stableTo(long i, long t) {
        long gt = maybeTerm(i);
        if (gt >= 0L) return;

        // if i < offset, term is matched with the snapshot
        // only update the unstable entries if term is matched with
        // an unstable entry.
        if (gt == t && i >= offset) {
            entries = Arrays.copyOfRange(entries, (int) (i + 1 - offset), entries.length - 1);
            offset = i + 1;
            shrinkEntriesArray();
        }
    }

    // shrinkEntriesArray discards the underlying array used by the entries slice
    // if most of it isn't being used. This avoids holding references to a bunch of
    // potentially large entries that aren't needed anymore. Simply clearing the
    // entries wouldn't be safe because clients might still be using them.
    public void shrinkEntriesArray() {
        // We replace the array if we're using less than half of the space in
        // it. This number is fairly arbitrary, chosen as an attempt to balance
        // memory usage vs number of allocations. It could probably be improved
        // with some focused tuning.
        final int lenMultiple = 2;
        if (entries.length == 0) {
            entries = null;
        } else {
            // TODO
        }
    }

    public void stableSnapTo(long i) {
        if (snapshot != null && snapshot.Metadata.Index == i) {
            snapshot = null;
        }
    }

    public void restore(pb.Snapshot s) {
        offset = s.Metadata.Index + 1;
        entries = null;
        snapshot = s;
    }
}

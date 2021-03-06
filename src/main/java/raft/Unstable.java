package raft;

import pb.Entry;
import util.ArrayUtil;

import java.lang.reflect.Array;
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
    public pb.Snapshot snapshot;
    // all entries that have not yet been written to storage.
    public pb.Entry[] entries = {};

    public long offset;

    public Logger logger;

    public long maybeLastIndex() {
        int l = entries.length;
        if (l != 0) {
            return offset + l - 1;
        }
        if (snapshot != null) {
            return snapshot.Metadata.Index;
        }
        return 0L;
    }

    public long maybeFirstIndex() {
        if (snapshot != null) {
            return snapshot.Metadata.Index + 1;
        }
        return 0L;
    }

    //  maybeTerm returns the term of the entry at index i, if there is any
    public long maybeTerm(long i) {
        if (i < offset) {
            if (snapshot == null) {
                return -1L;
            }
            if (this.snapshot.Metadata.Index == i) {
                return snapshot.Metadata.Term;
            }
            return -1L;
        }
        long last = this.maybeLastIndex();
        if (i > last) {
            return -1L;
        }
        return entries[Long.valueOf(i - offset).intValue()].Term;
    }

    public void truncateAndAppend(pb.Entry... ents) {
        long after = ents[0].Index;
        if (after == offset + entries.length) {
            // after is the next index in the u.entries
            // directly append
            logger.info(String.format("after %d is the next index in the u.entries directly append", after));
            entries = ArrayUtil.append(entries, ents);
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
            entries = ArrayUtil.append(ArrayUtil.asArray(Entry.builder().build()), slice(offset, after));
            entries = ArrayUtil.append(entries, ents);
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

    public Entry[] slice(long lo, long hi) {
        mustCheckOutOfBounds(lo, hi);
        return Arrays.copyOfRange(entries, (int) (lo - offset), (int) (hi - offset));
    }

    private void mustCheckOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            logger.panic(String.format("invalid unstable.slice %d > %d", lo, hi));
        }
        long upper = offset + entries.length;
        if (lo < offset || hi > upper) {
            logger.panic(String.format("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, offset, upper));
        }
    }

}

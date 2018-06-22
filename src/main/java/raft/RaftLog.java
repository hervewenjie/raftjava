package raft;

import com.alibaba.fastjson.JSON;
import pb.Entry;
import util.ArrayUtil;
import util.LOG;
import util.Panic;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public class RaftLog {
    // storage contains all stable entries since the last snapshot.
    public Storage storage;

    // unstable contains all unstable entries and snapshot.
    // they will be saved into storage.
    public Unstable unstable = new Unstable();

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes.
    public long committed;

    // applied is the highest log position that the application has
    // been instructed to apply to its state machine.
    // Invariant: applied <= committed
    public long applied;

    public Logger logger;

    RaftLog(Storage storage, Logger logger) {
        this.storage = storage;
        this.logger = logger;
    }

    // newLog returns log using the given storage. It recovers the log to the state
    // that it just commits and applies the latest snapshot.
    public static RaftLog newLog(Storage storage, Logger logger) {
        if (storage == null) {
            Panic.panic("storage must not be null");
        }

        RaftLog log = new RaftLog(storage, logger);
        long firstIndex = storage.FirstIndex();
        long lastIndex  = storage.LastIndex();
        log.unstable.offset = lastIndex + 1;
        log.unstable.logger = logger;
        // Initialize our committed and applied pointers to the time of the last compaction.
        log.committed = firstIndex - 1;
        log.applied   = firstIndex - 1;

        return log;
    }

    public String String() {
        return String.format("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d", committed, applied, unstable.offset, unstable.entries.length);
    }

    // maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
    // it returns (last index of new entries, true).
    public long maybeAppend(long index, long logTerm, long committed, pb.Entry... ents) {
        if (matchTerm(index, logTerm)) {
            long lastnewi = index + ents.length;
            long ci = findConflict(ents);
            if (ci == 0L || ci <= committed) {
                logger.panic(String.format("entry %d conflict with committed entry [committed(%d)]", ci, committed));
            } else {
                long offset = index + 1;
                append(Arrays.copyOfRange(ents, (int) (ci - offset), ents.length - 1));
            }
            commitTo(Math.min(committed, lastnewi));
            return lastnewi;
        }
        return 0L;
    }

    public boolean maybeCommit(long maxIndex, long term) {
        if (maxIndex > committed && zeroTermOnErrCompacted(term(maxIndex)) == term) {
            commitTo(maxIndex);
            return true;
        }
        return false;
    }

    public long append(pb.Entry... ents) {
        if (ents.length == 0) {
            return lastIndex();
        }
        long after = ents[0].Index - 1;
        if (after < committed) {
            logger.panic(String.format("after(%d) is out of range [committed(%d)]", after, committed));
        }
        unstable.truncateAndAppend(ents);
        return lastIndex();
    }

    // findConflict finds the index of the conflict.
    // It returns the first pair of conflicting entries between the existing
    // entries and the given entries, if there are any.
    // If there is no conflicting entries, and the existing entries contains
    // all the given entries, zero will be returned.
    // If there is no conflicting entries, but the given entries contains new
    // entries, the index of the first new entry will be returned.
    // An entry is considered to be conflicting if it has the same index but
    // a different term.
    // The first entry MUST have an index equal to the argument 'from'.
    // The index of the given entries MUST be continuously increasing.
    private long findConflict(pb.Entry[] ents) {
        for (int i = 0; i < ents.length; i++) {
            pb.Entry ne = ents[i];
            if (!matchTerm(ne.Index, ne.Term)) {
                if (ne.Index <= lastIndex()) {
                    this.logger.info(String.format("found conflict at index %d [existing term: %d, conflicting term: %d]",
                            ne.Index, zeroTermOnErrCompacted(term(ne.Index)), ne.Term));
                }
                return ne.Index;
            }
        }
        return 0L;
    }

    public pb.Entry[] unstableEntries() {
        if (unstable.entries.length == 0) return null;
        return unstable.entries;
    }

    // nextEnts returns all the available entries for execution.
    // If applied is smaller than the index of snapshot, it returns all committed
    // entries after the index of snapshot.
    public pb.Entry[] nextEnts() {
        long off = Math.max(this.applied + 1, this.firstIndex());
        if (this.committed + 1 > off) {
            pb.Entry[] ents = slice(off, committed + 1, Long.MAX_VALUE);
            return ents;
        }
        return null;
    }

    // hasNextEnts returns if there is any available entries for execution. This
    // is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
    public boolean hasNextEnts() {
        long off = Math.max(applied + 1, firstIndex());
        return committed + 1 > off;
    }

    public pb.Snapshot snapshot() {
        if (unstable.snapshot != null) {
            return unstable.snapshot;
        }
        return storage.Snapshot();
    }

    public long firstIndex() {
        long i;
        if ((i = this.unstable.maybeFirstIndex()) >= 0L) {
            return i;
        }
        i = this.storage.LastIndex();
        return i;
    }

    public long lastIndex() {
        long i = unstable.maybeLastIndex();
        if (i >= 0L) {
            return i;
        }
        i = storage.LastIndex();
        return i;
    }

    public void commitTo(Long tocommit) {
        // never decrease commit
        if (committed < tocommit) {
            if (lastIndex() < tocommit) {
                logger.panic(String.format("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, this.lastIndex()));
            }
            committed = tocommit;
        }
    }

    public void appliedTo(long i) {
        if (i == 0L) {
            return;
        }
        if (committed < i || i < applied) {
            logger.panic(String.format("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, applied, committed));
        }
        applied = i;
    }

    public void stableTo(long i, long t) {unstable.stableTo(i, t);}

    long lastTerm() {
        long t = this.term(this.lastIndex());
        return t;
    }

    long term(Long i) {
        long dummyIndex = this.firstIndex() - 1;
        if (i < dummyIndex || i > this.lastIndex()) {
            // TODO return an error instead???
            return 0L;
        }
        long t;
        if ((t = this.unstable.maybeTerm(i)) != 0L) {
            return t;
        }
        t = this.storage.term(i);
        if (t != 0L) {
            return t;
        }
        return 0L;
    }

    // slice returns a slice of log entries from lo through hi-1, inclusive.
    public pb.Entry[] slice(long lo, long hi, long maxSize) {
        mustCheckOutOfBounds(lo, hi);

        if (lo == hi) {
            return null;
        }

        Entry[] ents = null;
        if (lo < unstable.offset) {
            Entry[] storedEnts = storage.entries(lo, Math.min(hi, unstable.offset), maxSize);
            if (storedEnts == null) {
                logger.panic("storedEnts null");
            }

            // check if ents has reached the size limitation
            if (storedEnts.length < Math.max(hi, unstable.offset) - lo) {
                return storedEnts;
            }

            ents = storedEnts;
        }

        if (hi > unstable.offset) {
            Entry[] unstableE = unstable.slice(Math.max(lo, unstable.offset), hi);
            if (ents != null && ents.length > 0) {
                ents = ArrayUtil.append(ArrayUtil.asArray(Entry.builder().build()), ents);
                ents = ArrayUtil.append(ents, unstableE);
            } else {
                ents = unstableE;
            }
        }
        return Util.limitSize(ents, maxSize);
    }

    // l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
    private void mustCheckOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            logger.panic(String.format("invalid slice %d > %d", lo, hi));
        }

        long fi = firstIndex();
        if (lo < fi) {
            logger.panic("err compacted");
        }

        long length = lastIndex() + 1 - fi;
        if (lo < fi || hi > fi + length) {
            logger.panic(String.format("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, lastIndex()));
        }
    }

    // isUpToDate determines if the given (lastIndex,term) log is more up-to-date
    // by comparing the index and term of the last entries in the existing logs.
    // If the logs have last entries with different terms, then the log with the
    // later term is more up-to-date. If the logs end with the same term, then
    // whichever log has the larger lastIndex is more up-to-date. If the logs are
    // the same, the given log is up-to-date.
    public boolean isUpToDate(Long lasti, Long term) {
        return term > this.lastTerm() || (term == this.lastTerm() && lasti >= this.lastIndex());
    }

    public pb.Entry[] entries(long i, long maxsize) {
        LOG.debug("RaftLog entries lastIndex " + lastIndex());
        LOG.debug("RaftLog entries unstable " + JSON.toJSONString(unstableEntries()));

        if (i > lastIndex()) {
            return null;
        }
        return slice(i, lastIndex() + 1, maxsize);
    }

    public Long zeroTermOnErrCompacted(Long t) {
        return 0L;
    }

    // - - -
    // privates
    private boolean matchTerm(long i, long term) {
        long t = term(i);
        if (t == 0L) return false;
        return t == term;
    }

    public void stableSnapTo(long i) {
        unstable.stableSnapTo(i);
    }

}

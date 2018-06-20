package raft;

import javafx.util.Pair;
import pb.ConfState;
import pb.Entry;
import pb.HardState;
import pb.Snapshot;
import util.ArrayUtil;
import util.LOG;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by chengwenjie on 2018/5/31.
 */
public class MemoryStorage implements Storage {

    public pb.HardState hardState;
    public pb.Snapshot snapshot;
    // ents[i] has raft log position i+snapshot.Metadata.Index
    public pb.Entry[] ents;

    Lock lock = new ReentrantLock();

    @Override
    public long term(long i) {
        return 0L;
    }

    @Override
    public long LastIndex() {
        return 0L;
    }

    @Override
    public long FirstIndex() {
        return 0L;
    }

    @Override
    public Snapshot Snapshot() {
        return new Snapshot();
    }

    @Override
    public Pair<HardState, ConfState> initialState() {
        return new Pair<>(new HardState(), new ConfState());
    }

    @Override
    public Entry[] entries(long lo, long hi, long maxSize) {
        return new Entry[0];
    }

    public long firstIndex() {
        return ents[0].Index + 1;
    }

    public long lastIndex() {
        return ents[0].Index + ents.length - 1;
    }

    // ApplySnapshot overwrites the contents of this Storage object with
    // those of the given snapshot.
    public synchronized void ApplySnapshot(Snapshot snap) {
        if (snap == null) return;
        // handle check for old snapshot being applied
        long msIndex = snapshot.Metadata.Index;
        long snapIndex = snap.Metadata.Index;
        if (msIndex >= snapIndex) {
            return;
        }
        snapshot = snap;
        ents = new Entry[1];
        ents[0] = Entry.builder().Term(snap.Metadata.Term).Index(snap.Metadata.Index).build();
    }

    // Append the new entries to storage.
    // TODO (xiangli): ensure the entries are continuous and
    // entries[0].Index > ms.entries[0].Index
    public synchronized void Append(Entry[] entries) {
        if (entries == null || entries.length == 0) { return;}
        long first = firstIndex();
        long last = entries[0].Index + entries.length - 1;
        // shortcut if there is no new entry.
        if (last < first) {
            return;
        }

        // truncate compacted entries
        if (first > entries[0].Index) {
            entries = Arrays.copyOfRange(entries, (int) (first - entries[0].Index), entries.length - 1);
        }

        long offset = entries[0].Index - ents[0].Index;
        if (ents.length > offset) {
            ents = ArrayUtil.append(new Entry[0], entries);
            ents = ArrayUtil.append(ents, entries);
        } else if (ents.length == offset) {
            ents = ArrayUtil.append(ents, entries);
        } else {
            LOG.error(String.format("missing log entry [last: %d, append at: %d]",
                    lastIndex(), entries[0].Index));
        }

    }
}

package example;

import javafx.util.Pair;
import lombok.Builder;
import pb.ConfChange;
import pb.ConfState;
import pb.Entry;
import pb.Snapshot;
import raft.*;
import time.Tiker;
import util.Encoder;
import util.LOG;
import util.Panic;
import wal.WAL;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * A key-value stream backed by raft
 *
 * Created by chengwenjie on 2018/5/31.
 */
@Builder
public class RaftNode {
    Queue<String> proposeC;        // proposed messages (k,v)
    Queue<ConfChange>     confChangeC;     // proposed cluster config changes
    Queue<KV> commitC;         // entries committed to log (k,v)
    Queue     errorC;          // errors from raft session

    int          id;           // client ID for raft session
    String[]     peers;        // raft peer URLs
    boolean      join;         // nodeImpl is joining an existing cluster
    String       waldir;       // path to WAL directory
    String       snapdir;      // path to snapshot directory
    SnapshotFunc snapshotFunc;
    long         lastIndex;    // index of log at start

    ConfState confState;
    long snapshotIndex;
    long appliedIndex;

    // raft backing for the commit/error channel
    NodeImpl nodeImpl;
    @Builder.Default
    raft.MemoryStorage raftStorage = new MemoryStorage();
    WAL wal;

    SnapShotter snapShotter;
    Queue<SnapShotter> snapshotterReady; // signals when snapshotter is ready

    long snapCount;
    Transport transport;

    Queue stopc;            // signals proposal channel closed
    Queue httpstopc;        // signals http server to shutdown
    Queue httpdonec;        // signals http server shutdown complete

    public void startRaft() {
        File snapdir = new File(this.snapdir);
        if (!snapdir.exists()) {
            try {
                boolean b = snapdir.createNewFile();
                if (!b) Panic.panic("raftexample: cannot create dir for snapshot");
            } catch (IOException e) {
                Panic.panic("raftexample: cannot create dir for snapshot");
            }
        }
        snapShotter = SnapShotter.builder()
                .lg(new LoggerImpl())
                .dir(this.snapdir)
                .build();
        snapshotterReady.add(snapShotter);

        boolean oldWal = WAL.exists(this.waldir);
        wal = replayWal();

        Peer[] rpeers = new Peer[peers.length];
        for (int i = 0; i < rpeers.length; i++) {
            rpeers[i] = Peer.builder().ID((long) i + 1).build();
        }

        Config c = Config.builder()
                .ID((long) id)
                .electionTick(10)
                .heartbeatTick(3)
                .storage(raftStorage)
                .maxSizePerMsg(1024L * 1024L)
                .maxInflightMsgs(256)
                .logger(new LoggerImpl())
                .storage(new MemoryStorage())
                .build();

       if (oldWal) {
           this.nodeImpl = NodeImpl.restartNode(c);
       } else {
           Peer[] startPeers = rpeers;
           if (join) {
               startPeers = null;
           }
           this.nodeImpl = NodeImpl.startNode(c, startPeers);
       }

       transport = Transport.builder()
               .logger(new LoggerImpl())
               .ID(Long.valueOf(id))
               .ClusterID(0x1000L)
               .raft(this)
//                ServerStats: stats.NewServerStats("", ""),
//                LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
               .build();

        transport.start(id);

        // hard coded peer
//        for (int i = 0; i < rpeers.length; i++) {
//            if (i + 1 != this.id) {
//                // transport add peer
//            }
//        }

        // mainly start listener
        new Thread(() -> this.serveRaft()).start();
        //
        new Thread(() -> this.serveChannels()).start();
    }

    public wal.WAL replayWal() {
        LOG.info(String.format("replaying WAL of member %d", id));
        Snapshot snapshot = loadSnapshot();
        WAL w = openWAL(snapshot);
        w.ReadAll();
        raftStorage = NewMemoryStorage();
        if (snapshot != null) {
            raftStorage.ApplySnapshot(snapshot);
        }
        // TODO
        return w;
    }

    public MemoryStorage NewMemoryStorage() {
        MemoryStorage m = new MemoryStorage();
        // When starting from scratch populate the list with a dummy entry at term zero.
        m.ents = new Entry[1];
        m.ents[0] = Entry.builder().build();
        return m;
    }

    public WAL openWAL(Snapshot snapshot) {
        return new WAL();
    }

    // what for???
    public void serveRaft() {
        LOG.info("server Raft...");
        // listener & http server.serve

    }

    public void serveChannels() {
        Snapshot snapshot = raftStorage.Snapshot();

        confState = snapshot.Metadata.ConfState;
        snapshotIndex = snapshot.Metadata.Index;
        appliedIndex = snapshot.Metadata.Index;

        Tiker tiker = new Tiker();

        // send proposals over raft
        new Thread(() -> {
            long confChangeCount = 0L;
            while (true) {
                if (proposeC.peek() != null) {
                    // blocks until accepted by raft state machine
                    KV kv = Encoder.decode(proposeC.poll().getBytes());
                    LOG.debug("serveChannels kv " + kv);
                    nodeImpl.Propose(null, Encoder.encode(kv));
                }
                else if (confChangeC.peek() != null) {

                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.error("serveChannels interrupted");
                }
            }
        }).start();

        // event loop on raft state machine updates
        while (true) {
            // tick
            if (tiker.isTimeout()) {
                nodeImpl.Tick();
            }

            // store raft entries to wal, then publish over commit channel
            if (nodeImpl.Ready().peek() != null) {
                LOG.debug("RaftNode got Ready...");
                Ready rd = nodeImpl.Ready().poll();
                wal.Save(rd.HardState, rd.Entries);
                if (Ready.IsEmptySnap(rd.Snapshot)) {
                    saveSnap(rd.Snapshot);
                    raftStorage.ApplySnapshot(rd.Snapshot);
                    publishSnapshot(rd.Snapshot);
                }
                // Append!! & Send!!
                raftStorage.Append(rd.Entries);
                transport.Send(rd.Messages);
                publishEntries(entriesToApply(rd.CommittedEntries));
                maybeTriggerSnapshot();

                // notify node advance
                nodeImpl.Advance();
            }

            // else stop error etc...
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
        }

    }

    public void maybeTriggerSnapshot() {

    }

    public void saveSnap(Snapshot snap) {

    }

    public Entry[] entriesToApply(Entry[] ents) {
        return null;
    }

    public boolean publishEntries(Entry[] ents) {
        return false;
    }

    public Snapshot loadSnapshot() {
        return null;
    }

    public void publishSnapshot(Snapshot snapshotToSave) {
        if (Ready.IsEmptySnap(snapshotToSave)) {return;}

        LOG.debug(String.format("publishing snapshot at index %d", snapshotIndex));

        if (snapshotToSave.Metadata.Index <= appliedIndex) {
            LOG.error(String.format("snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, appliedIndex));
        }

        // trigger kvstore to load snapshot
        commitC.add(new KV("", ""));

        confState = snapshotToSave.Metadata.ConfState;
        snapshotIndex = snapshotToSave.Metadata.Index;
        appliedIndex = snapshotToSave.Metadata.Index;

        LOG.debug(String.format("finished publishing snapshot at index %d", snapshotIndex));
    }

    static long defaultSnapshotCount = 10000;

    // newRaftNode initiates a raft instance and returns a committed log entry
    // channel and error channel. Proposals for log updates are sent over the
    // provided the proposal channel. All log entries are replayed over the
    // commit channel, followed by a nil message (to indicate the channel is
    // current), then new log entries. To shutdown, close proposeC and read errorC.
    public static Pair<Queue<KV>, Queue<SnapShotter>> newRaftNode(int id, String[] peers, boolean join,
                                                                  SnapshotFunc snapshotFunc,
                                                                  Queue<String> proposeC,
                                                                  Queue confChangeC) {
        Queue<KV> commitC = new ArrayBlockingQueue<>(128);
        Queue<KV> errorC  = new ArrayBlockingQueue<>(128);

        RaftNode rc = RaftNode.builder()
                .proposeC(proposeC)
                .confChangeC(confChangeC)
                .commitC(commitC)
                .errorC(errorC)
                .id(id)
                .peers(peers)
                .join(join)
                .waldir(String.format("raftexample-%d", id))
                .snapdir(String.format("raftexample-%d-snap", id))
                .snapshotFunc(snapshotFunc)
                .snapCount(defaultSnapshotCount)
                // stopc
                // httpstopc
                // httpdonec
                .snapshotterReady(new ArrayBlockingQueue<>(1))
                .build();
        // rest of structure populated after WAL replay

        new Thread(() -> rc.startRaft()).start();

        return new Pair<>(commitC, rc.snapshotterReady);
    }
}

package raft;

import com.alibaba.fastjson.JSON;
import context.Context;
import javafx.scene.paint.Stop;
import lombok.Builder;
import pb.*;
import util.ArrayUtil;
import util.LOG;
import util.Panic;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * NodeImpl represents a node in a raft cluster
 *
 * Created by chengwenjie on 2018/5/30.
 */
@Builder
public class NodeImpl implements Node {

    public Queue<MessageWithResult> propc;         //  chan msgWithResult
    public Queue<pb.Message>        recvc;         //  chan pb.Message
    public Queue<pb.ConfChange>     confc;         //  chan pb.ConfChange
    public Queue<pb.ConfState>      confstatec;    //  chan pb.ConfState
    public Queue<Ready> readyc;                    //  chan Ready
    public Queue advancec;                         //  chan struct{}
    public Queue tickc;                            //  chan struct{}
    public Queue done;                             //  chan struct{}
    public Queue stop;                             //  chan struct{}
    public Queue<Queue> status;                    //  chan chan Status

    Logger logger;

    public static pb.HardState emptyState = HardState.builder().build();

    public static Boolean isHardStateEqual(pb.HardState a, pb.HardState b) {
        return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit;
    }

    // StartNode returns a new NodeImpl given configuration and a list of raft peers.
    // It appends a ConfChangeAddNode entry for each given peer to the initial log.
    public static NodeImpl startNode(Config c, Peer[] peers) {
        Raft r = Raft.newRaft(c);
        // become the follower at term 1
        r.becomeFollower(1L, 0L);
        // apply initial configuration
        // entries of term 1
        for (int i = 0; i < peers.length; i++) {
            ConfChange cc = pb.ConfChange.builder()
                    .type(ConfChangeType.ConfChangeAddNode)
                    .NodeID(peers[i].ID)
                    .Context(peers[i].Context)
                    .build();
            byte[] d = cc.Marshal();
            if (d == null) Panic.panic("unexpected marshal error");

            Entry e = pb.Entry.builder()
                    .Type(EntryType.EntryConfChange)
                    .Term(1L)
                    .Index(r.raftLog.lastIndex() + 1)
                    .Data(d)
                    .build();
            r.raftLog.append(e);
        }

        // Mark these initial entries as committed.
        // TODO(bdarnell): These entries are still unstable; do we need to preserve
        // the invariant that committed < unstable?
        r.raftLog.committed = r.raftLog.lastIndex();
        // Now apply them, mainly so that the application can call Campaign
        // immediately after StartNode in tests. Note that these nodes will
        // be added to raft twice: here and when the application's Ready
        // loop calls ApplyConfChange. The calls to addNode must come after
        // all calls to raftLog.append so progress.next is set after these
        // bootstrapping entries (it is an error if we try to append these
        // entries since they have already been committed).
        // We do not set raftLog.applied so the application will be able
        // to observe all conf changes via Ready.CommittedEntries.
        for (int i = 0; i < peers.length; i++) {
            r.addNode(peers[i].ID);
        }

        NodeImpl n = newNode();
        n.logger = c.logger;
        new Thread(() -> n.run(r)).start();
        return n;
    }

    public static NodeImpl newNode() {
        return NodeImpl.builder()
                .propc(new ArrayBlockingQueue(128))
                .recvc(new ArrayBlockingQueue(128))
                .confc(new ArrayBlockingQueue(128))
                .confstatec(new ArrayBlockingQueue(128))
                .readyc(new ArrayBlockingQueue(128))
                .advancec(new ArrayBlockingQueue(128))
                // make tickc a buffered chan, so raft node can buffer some ticks when the node
                // is busy processing raft messages. Raft node will resume process buffered
                // ticks when it becomes idle.
                .tickc(new ArrayBlockingQueue(128))
                .done(new ArrayBlockingQueue(128))
                .stop(new ArrayBlockingQueue(128))
                .status(new ArrayBlockingQueue(128))
                .build();
    }

    public void run(Raft r) {
        Queue<MessageWithResult> propc = new ArrayBlockingQueue<>(1);
        Queue<Ready>             readyc;
        Queue                    advancec = null;
        long prevLastUnstablei = 0L;
        long prevLastUnstablet = 0L;
        boolean havePrevLastUnstablei = false;
        long prevSnapi = 0L;
        Ready rd = null;

        long lead = 0L;
        SoftState prevSoftSt = r.softState();
        HardState prevHardSt = emptyState;

        // we want all if statement to run, do not if else
        for (;;) {
            // deal with...
            if (advancec != null) {
                readyc = null;
            } else {
                rd = newReady(r, prevSoftSt, prevHardSt);
                if (rd.containsUpdates()) {
                    readyc = this.readyc;
                } else {
                    readyc = null;
                }
            }

            // must be leader here
            if (lead != r.lead) {
                if (r.hasLeader()) {
                    if (lead > 0L) {
                        r.logger.info(String.format("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term));
                    } else {
                        r.logger.info(String.format("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term));
                    }
                    propc = this.propc;
                } else {
                    r.logger.info(String.format("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term));
                    propc = null;
                }
                lead = r.lead;
            }

            // select...
            // TODO: maybe buffer the config propose if there exists one (the way described in raft dissertation)
            // Currently it is dropped in Step silently.

            // propose
            if (propc.peek() != null) {
                LOG.debug("node run propc peek...");
                MessageWithResult pm = propc.poll();
                Message m = pm.m;
                m.From = r.id;
                r.Step(m);
                if (pm.result != null) {
                    // TODO
                }

            }
            // receive message?
            if (recvc.peek() != null) {
                Message m = recvc.poll();
                Progress pr = r.getProgress(m.From);
                if (pr != null || Util.IsResponseMsg(m.Type)) {
                    r.Step(m);
                }

                continue;
            }

            if (confc.peek() != null) {

            }
            // tick
            if (tickc.peek() != null) {
                tickc.poll();
                r.tick.tick(r);

                // fucking hack
                continue;
            }

            // ready
            if (readyc != null && rd != null && rd.containsUpdates()) {
                readyc = this.readyc;
                readyc.add(rd);
                if (rd.SoftState != null) {
                    prevSoftSt = rd.SoftState;
                }
                if (rd.Entries != null && rd.Entries.length > 0) {
                    prevLastUnstablei = rd.Entries[rd.Entries.length - 1].Index;
                    prevLastUnstablet = rd.Entries[rd.Entries.length - 1].Term;
                    havePrevLastUnstablei = true;
                }
                if (!rd.IsEmptyHardState(rd.HardState)
                        // hack???
                        && rd.Snapshot != null) {
                    prevSnapi = rd.Snapshot.Metadata.Index;
                }

                // empty message
                r.msgsClear();
                r.readStates = null;
                advancec = this.advancec;
            }

            if (advancec != null && advancec.peek() != null) {
                if (prevHardSt.Commit != 0) {
                    r.raftLog.appliedTo(prevHardSt.Commit);
                }
                if (havePrevLastUnstablei) {
                    r.raftLog.stableTo(prevLastUnstablei, prevLastUnstablet);
                    havePrevLastUnstablei = false;
                }
                r.raftLog.stableSnapTo(prevSnapi);
                advancec = null;
            }

            if (status.peek() != null) {

            }

            if (stop.peek() != null) {

            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
        }

    }

    public static NodeImpl restartNode(Config c) {
        return null;
    }

    // ================================================

    private void step(pb.Message m) {
        this.stepWithWaitOption(m, false);
    }

    private void stepWait(pb.Message m) {
        this.stepWithWaitOption(m, true);
    }

    // Step advances the state machine using msgs. The ctx.Err() will be returned,
    // basically add message to propose channel
    private void stepWithWaitOption(pb.Message m, boolean wait) {
        if (m.Type != MessageType.MsgProp) {
            recvc.add(m);
        }

        Queue<MessageWithResult> ch = propc;
        MessageWithResult pm = MessageWithResult.builder().m(m).build();
        if (wait) {
            pm.result = new ArrayBlockingQueue(1);
        }

        // add to channel, fucked up logic
        while (true) {
            ch.add(pm);
            if (!wait) return;
            else break;
        }

        // wait
        while (true) {
            LOG.debug("step with option, waiting...");
            MessageWithResult rsp = pm.result.poll();
            if (rsp != null) return;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
        }
    }

    public static Ready newReady(Raft r, SoftState prevSoftSt, pb.HardState prevHardSt) {
        Ready rd = Ready.builder()
                .Entries(r.raftLog.unstableEntries())
                .CommittedEntries(r.raftLog.nextEnts())
                .Messages(r.msgs())
                .build();

        SoftState softSt = r.softState();
        if (!softSt.equals(prevSoftSt)) {
            rd.SoftState = softSt;
        }
        HardState hardSt = r.hardState();
        if (!isHardStateEqual(hardSt, prevHardSt)) {
            rd.HardState = hardSt;
        }
        if (r.raftLog.unstable.snapshot != null) {
            rd.Snapshot = r.raftLog.unstable.snapshot;
        }
        if (r.readStates != null && r.readStates.length != 0) {
            rd.ReadStates = r.readStates;
        }
        rd.MustSync = MustSync(rd.HardState, prevHardSt, rd.Entries.length);
        return rd;
    }

    @Override
    public void ReportUnreachable(long id) {

    }

    @Override
    public void ReportSnapshot(long id, SnapshotStatus status) {

    }

    @Override
    public void Campaign(Context ctx) {
        this.step(pb.Message.builder().Type(MessageType.MsgHup).build());
    }

    @Override
    public void Propose(Context ctx, byte[] data) {
        Entry[] entries = new Entry[1];
        entries[0] = Entry.builder().Data(data).build();
        this.stepWait(pb.Message.builder().Type(MessageType.MsgProp)
                .Entries(entries).build());
    }

    @Override
    public void ProposeConfChange(Context ctx, ConfChange cc) {

    }

    @Override
    public void ReadIndex(Context ctx, byte[] rctx) {
        pb.Entry[] entries = new pb.Entry[1];
        entries[0] = pb.Entry.builder().Data(rctx).build();
        this.step(pb.Message.builder().Type(MessageType.MsgReadIndex).Entries(entries).build());
    }

    @Override
    public void Step(Context ctx, Message msg) {
        this.stepWithWaitOption(msg, false);
    }

    @Override
    public void TransferLeadership(Context ctx, long lead, long transferee) {

    }

    @Override
    public void Tick() {
        tickc.add(new Object()); // signal one tick
    }

    @Override
    synchronized public void Advance() {
        advancec.add(new Object());
    }

    @Override
    public void Stop() {
        stop.add(new Object());
    }

    @Override
    public Queue<Ready> Ready() {
        return readyc;
    }

    @Override
    public Status Status() {
        return null;
    }

    @Override
    public ConfState ApplyConfChange(ConfChange cc) {
        return null;
    }

    // MustSync returns true if the hard state and count of Raft entries indicate
    // that a synchronous write to persistent storage is required.
    public static boolean MustSync(HardState st, HardState prevst, int entsnum) {
        return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term;
    }
}

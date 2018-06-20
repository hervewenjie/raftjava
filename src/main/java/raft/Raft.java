package raft;

import javafx.util.Pair;
import lombok.Builder;
import pb.*;
import util.ArrayUtil;
import util.LOG;
import util.Panic;

import java.util.*;

import static pb.MessageType.*;
import static raft.ProgressStateType.ProgressStateReplicate;
import static raft.ReadOnlyOption.ReadOnlySafe;

/**
 * Created by chengwenjie on 2018/5/29.
 */
@Builder
public class Raft {
    long id;

    long Term;
    long Vote;

    ReadState[] readStates;

    // the log
    RaftLog raftLog;

    int maxInflight;
    long    maxMsgSize;
    @Builder.Default
    Map<Long, Progress> prs = new HashMap<>();
    @Builder.Default
    Map<Long, Progress> learnerPrs = new HashMap<>();

    StateType state;

    // isLearner is true if the local raft node is a learner.
    boolean isLearner;

    Map<Long, Boolean> votes;

    @Builder.Default
    final List<Message> msgs = new ArrayList<>();

    // the leader id
    long lead;

    // leadTransferee is id of the leader transfer target when its value is not zero.
    // Follow the procedure defined in raft thesis 3.10.
    long leadTransferee;

    // Only one conf change may be pending (in the log, but not yet
    // applied) at a time. This is enforced via pendingConfIndex, which
    // is set to a value >= the log index of the latest pending
    // configuration change (if any). Config changes are only allowed to
    // be proposed if the leader's applied index is greater than this
    // value.
    long pendingConfIndex;

    ReadOnly readOnly;

    // number of ticks since it reached last electionTimeout when it is leader
    // or candidate.
    // number of ticks since it reached last electionTimeout or received a
    // valid message from current leader when it is a follower.
    int electionElapsed;

    // number of ticks since it reached last heartbeatTimeout.
    // only leader keeps heartbeatElapsed.
    int heartbeatElapsed;

    boolean checkQuorum;
    boolean preVote;

    int heartbeatTimeout;
    int electionTimeout;

    // randomizedElectionTimeout is a random number between
    // [electiontimeout, 2 * electiontimeout - 1]. It gets reset
    // when raft changes its state to follower or candidate.
    int randomizedElectionTimeout;
    boolean disableProposalForwarding;

    tick tick;
    stepFunc step;

    Logger logger;

    public static Raft newRaft(Config c) {
        RaftLog raftLog = Log.newLog(c.storage, c.logger);

        Pair<HardState, ConfState> statePair = c.storage.initialState();
        HardState hs = statePair.getKey();
        ConfState cs = statePair.getValue();

        long[] peers = c.peers;
        long[] learners = c.learners;
        if (cs.nodes.length > 0 || cs.learners.length > 0) {
            if (peers.length > 0 || learners.length > 0) {
                Panic.panic("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)");
            }
            peers = cs.nodes;
            learners = cs.learners;
        }

        // init raft instance
        Raft r = Raft.builder()
                .id(c.ID)
                .lead(0L)
                .isLearner(false)
                .raftLog(raftLog)
                .maxMsgSize(c.maxSizePerMsg)
                .maxInflight(c.maxInflightMsgs)
                .electionTimeout(c.electionTick)
                .heartbeatTimeout(c.heartbeatTick)
                .logger(c.logger)
                .checkQuorum(c.checkQuorum)
                .preVote(c.preVote)
                .readOnly(ReadOnly.newReadOnly(c.readOnlyOption))
                .disableProposalForwarding(c.disableProposalForwarding)
                .build();

        // init peers from config
        for (long p : peers) {
            r.prs.put(p, Progress.builder().Next(1L).ins(Inflights.newInflights(r.maxInflight)).build());
        }
        // init learners form config
        for (long l : learners) {
            if (r.prs.get(l) != null) {
                Panic.panic(String.format("node %x is in both learner and peer list", l));
            }
            r.learnerPrs.put(l, Progress.builder().Next(1L).ins(Inflights.newInflights(r.maxInflight)).isLearner(true).build());
            if (r.id == l) {
                r.isLearner = true;
            }
        }

        // load hard state if not empty
        if (!NodeImpl.isHardStateEqual(hs, NodeImpl.emptyState)) {
            r.loadState(hs);
        }
        // apply last index if restart
        if (c.applied > 0) {
            raftLog.appliedTo(c.applied);
        }

        // - - - - - - - - - - - - -
        // now become follower with no lead!
        r.becomeFollower(r.Term, 0L);
        // - - - - - - - - - - - - -

        List<String> nodesStrs = new ArrayList<>();
        for (Long n : r.nodes()) {
            nodesStrs.add(String.valueOf(n));
        }

        // i am alive!
        r.logger.info(String.format("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
                r.id, String.join(",", nodesStrs), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm()));
        return r;
    }

    void loadState(pb.HardState state) {
        if (state.Commit < this.raftLog.committed || state.Commit > this.raftLog.lastIndex()) {
            Panic.panic(String.format("%x state.commit %d is out of range [%d, %d]", id, state.Commit, raftLog.committed, raftLog.lastIndex()));
        }
    }

    private void resetRandomizedElectionTimeout() {
        Random random = new Random();
        randomizedElectionTimeout = electionTimeout + random.nextInt(electionTimeout);
        // hack! make sure 1 always time out
        randomizedElectionTimeout = (int) (randomizedElectionTimeout * id * id);
    }

    void abortLeaderTransfer() {
        leadTransferee = 0L;
    }

    // handle entries in received message
    public void handleAppendEntries(pb.Message m) {
        // entries already committed
        if (m.Index < this.raftLog.committed) {
            this.send(pb.Message.builder().To(m.From).Type(MessageType.MsgAppResp).Index(this.raftLog.committed).build());
            return;
        }

        // append entries here
        long mlastIndex = raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries);
        if (mlastIndex != 0L) {
            // append succeeds, send response
            send(pb.Message.builder().To(m.From).Type(MsgAppResp).Index(raftLog.committed).build());
        } else {
            logger.debug(String.format("%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
                    id, raftLog.zeroTermOnErrCompacted(raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From));
            send(pb.Message.builder().To(m.From).Type(MsgAppResp).Index(m.Index).Reject(true).RejectHint(raftLog.lastIndex()).build());
        }
    }

    public void handleHeartbeat(pb.Message m) {
        raftLog.commitTo(m.Commit);
        send(pb.Message.builder().To(m.From).Type(MessageType.MsgHeartbeatResp).Context(m.Context).build());
    }

    public void handleSnapshot(pb.Message m) {

    }

    // --------

    public boolean hasLeader() {
        return lead != 0L;
    }

    public SoftState softState() {
        return SoftState.builder().Lead(lead).RaftState(state).build();
    }

    public int quorum() {
        return prs.size() / 2 + 1;
    }

    public long[] nodes() {
        List<Long> nodes = new ArrayList<>();
        this.prs.keySet().stream().forEach(id -> nodes.add(id));
        Collections.sort(nodes);
        long[] n = new long[nodes.size()];
        for (int i = 0; i < n.length; i++) n[i] = nodes.get(i);
        return n;
    }

    public Long[] learnerNodes() {
        List<Long> nodes = new ArrayList<>();
        learnerPrs.keySet().stream().forEach(i -> nodes.add(i));
        Collections.sort(nodes);
        return (Long[]) nodes.toArray();
    }

    // send persists state to stable storage and then sends to its mailbox.
    public void send(pb.Message m) {
        m.From = this.id;
        if (m.Type == MessageType.MsgVote || m.Type == MessageType.MsgVoteResp || m.Type == MessageType.MsgPreVote || m.Type == MessageType.MsgPreVoteResp) {
            if (m.Term == 0) {
                // All {pre-,}campaign messages need to have the term set when
                // sending.
                // - MsgVote: m.Term is the term the node is campaigning for,
                //   non-zero as we increment the term when campaigning.
                // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
                //   granted, non-zero for the same reason MsgVote is
                // - MsgPreVote: m.Term is the term the node will campaign,
                //   non-zero as we use m.Term to indicate the next term we'll be
                //   campaigning for
                // - MsgPreVoteResp: m.Term is the term received in the original
                //   MsgPreVote if the pre-vote was granted, non-zero for the
                //   same reasons MsgPreVote is
                Panic.panic(String.format("term should be set when sending %s", m.Type));
            }
        } else {
            if (m.Term != 0) {
                Panic.panic(String.format("term should not be set when sending %s (was %d)", m.Type, m.Term));
            }
            // do not attach term to MsgProp, MsgReadIndex
            // proposals are a way to forward to the leader and
            // should be treated as local message.
            // MsgReadIndex is also forwarded to leader.
            if (m.Type != MessageType.MsgProp && m.Type != MessageType.MsgReadIndex) {
                m.Term = this.Term;
            }
        }
        // append to array, if no space, new array is allocated
        msgs.add(m);
    }

    public Progress getProgress(long id) {
        Progress p;
        if ((p = prs.get(id)) != null) return p;
        return learnerPrs.get(id);
    }

    // sendAppend sends RPC, with entries to the given peer.
    public void sendAppend(long to) {
        Progress pr = getProgress(id);
        if (pr.IsPaused()) return;

        Message m = pb.Message.builder().build();
        m.To = to;

        Long term = raftLog.term(pr.Next - 1);
        pb.Entry[] ents = raftLog.entries(pr.Next, maxMsgSize);

        if (term == null || ents == null) {
            // send snapshot if we failed to get term or entries
            // TODO

        } else {
            m.Type = MessageType.MsgApp;
            m.Index = pr.Next - 1;
            m.LogTerm = term;
            m.Entries = ents;
            m.Commit  = raftLog.committed;
            int n = m.Entries.length;
            if (n != 0) {
                switch (pr.State) {
                    case ProgressStateReplicate:
                        Long last = m.Entries[n - 1].Index;
                        pr.optimisticUpdate(last);
                        pr.ins.add(last);
                    case ProgressStateProbe:
                        pr.pause();
                    default:
                        logger.panic(String.format("%x is sending append in unhandled state %s", id, pr.State));
                }
            }
        }

        send(m);
    }

    // sendHeartbeat sends an empty MsgApp
    private void sendHeartbeat(Long to, byte[] ctx) {
        // Attach the commit as min(to.matched, r.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        long commit = Math.min(getProgress(to).Match, raftLog.committed);
        Message m = pb.Message.builder()
                .To(to)
                .Type(MessageType.MsgHeartbeat)
                .Commit(commit)
                .Context(ctx)
                .build();
        this.send(m);
    }

    void forEachProgress(ProgressHandler h) {
        prs.keySet().stream().forEach(k -> h.handle(k, prs));
        learnerPrs.keySet().stream().forEach(k -> h.handle(k, learnerPrs));
    }

    // bcastAppend sends RPC, with entries to all peers that are not up-to-date
    // according to the progress recorded in r.prs.
    public void bcastAppend() {
        this.forEachProgress((id, m) -> {
            // skip itself
            if (id.equals(id)) return;
            sendAppend(id);
        });
    }

    // bcastHeartbeat sends RPC, without entries to all the peers.
    public void bcastHeartbeat() {
        String lastCtx = readOnly.lastPendingRequestCtx();
        if (lastCtx == null || lastCtx.length() == 0) {
            bcastHeartbeatWithCtx(null);
        } else {
            bcastHeartbeatWithCtx(lastCtx.getBytes());
        }
    }

    public void bcastHeartbeatWithCtx(byte[] ctx) {
        forEachProgress((id, m) -> {
            if (id == this.id) return;
            sendHeartbeat(id, ctx);
        });
    }

    // maybeCommit attempts to advance the commit index. Returns true if
    // the commit index changed (in which case the caller should call
    // r.bcastAppend).
    public boolean maybeCommit() {
        long[] mis = new long[prs.size()];
        int i = 0;
        for (Long l : prs.keySet()) {
            mis[i] = prs.get(l).Match;
            i++;
        }
        long mci = mis[quorum() - 1];
        return raftLog.maybeCommit(mci, Term);
    }

    private void reset(long term) {
        // start a new term
        if (this.Term != term) {
            this.Term = term;
            Vote = 0L;
        }
        lead = 0L;

        electionElapsed = 0;
        heartbeatElapsed = 0;
        // random election timeout
        resetRandomizedElectionTimeout();

        abortLeaderTransfer();

        votes = new HashMap<>();
        // reset all progress
        forEachProgress((id, m) -> {
            Progress pr = Progress.builder().Next(raftLog.lastIndex() + 1).ins(Inflights.newInflights(maxInflight)).isLearner(isLearner).build();
            m.put(id, pr);
            if (id == this.id) {
                pr.Match = raftLog.lastIndex();
            }
        });

        pendingConfIndex = 0L;
        readOnly = ReadOnly.newReadOnly(readOnly.option);
    }

    // leader proposal append entry
    public void appendEntry(pb.Entry... es) {
        long li = raftLog.lastIndex();
        // calculate entry term and index
        for (int i = 0; i < es.length; i++) {
            es[i].Term  = Term;       // current term
            es[i].Index = li + 1 + i; // index increment
        }
        // use latest "last" index after truncate/append
        // actually put entries in unstable
        li = raftLog.append(es);
        // update progress
        getProgress(id).maybeUpdate(li);
        // Regardless of maybeCommit's return, our caller will call bcastAppend.
        maybeCommit();
    }

    public void becomeFollower(long term, long lead) {
        this.step = new stepFollower();
        this.reset(term);
        this.tick = new tickElection();
        this.lead = lead;
        this.state = StateType.StateFollower;
        this.logger.info(String.format("%x became follower at term %d", id, Term));
    }

    public void becomeCandidate() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (state == StateType.StateLeader) {
            Panic.panic("invalid transition [leader -> candidate]");
        }
        step = new stepCandidate();
        // for next term???
        reset(Term + 1);
        tick = new tickElection();
        Vote = id;
        state = StateType.StateCandidate;
        logger.info(String.format("%x became candidate at term %d", id, Term));
    }

    public void becomePreCandidate() {

    }

    public void becomeLeader() {
        if (this.state == StateType.StateFollower) {
            Panic.panic("invalid transition [follower -> leader]");
        }
        this.step = new stepLeader();
        this.reset(this.Term);
        this.tick = new tickHeartbeat();
        this.lead = this.id;
        this.state = StateType.StateLeader;
        Entry[] ents = this.raftLog.entries(raftLog.committed + 1, Long.MAX_VALUE);

        // Conservatively set the pendingConfIndex to the last index in the
        // log. There may or may not be a pending config change, but it's
        // safe to delay any future proposals until we commit all our
        // pending log entries, and scanning the entire tail of the log
        // could be expensive.
        if (ents != null && ents.length > 0) {
            this.pendingConfIndex = ents[ents.length - 1].Index;
        }
        this.appendEntry(Entry.builder().Data(null).build());
        logger.info(String.format("%x became leader at term %d", id, Term));
    }

    public void campaign(CampaignType t) {
        long term;
        MessageType voteMsg;

        if (t == CampaignType.campaignPreElection) {
            this.becomePreCandidate();
            voteMsg = MessageType.MsgPreVote;
            // PreVote RPCs are sent for the next term before we've incremented r.Term.
            term = this.Term + 1;
        } else {
            becomeCandidate();
            voteMsg = MessageType.MsgVote;
            term = Term;
        }

        // - - - - - - - - - -
        // new leader election

        // first vote for itself
        if (quorum() == poll(id, Util.voteRespMsgType(voteMsg), true)) {
            if (t == CampaignType.campaignPreElection) {
                campaign(CampaignType.campaignElection);
            } else {
                becomeLeader();
            }
            return;
        }

        for (long id : prs.keySet()) {
            // skip itself
            if (id == this.id) continue;
            logger.info(String.format("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
                    this.id, raftLog.lastTerm(), raftLog.lastIndex(), voteMsg, id, Term));

            byte[] ctx = null;
            if (t == CampaignType.campaignTransfer) {
                // TODO
                ctx = new byte[1];
            }
            // send vote (for itself) to peers
            // add to object's messages actually
            send(pb.Message.builder().Term(term).To(id).Type(voteMsg).Index(raftLog.lastIndex()).LogTerm(raftLog.lastTerm()).Context(ctx).build());
        }
    }

    public int poll(long id, pb.MessageType t, boolean v) {
        if (v) {
            logger.info(String.format("%x received %s from %x at term %d", this.id, t, id, Term));
        } else {
            logger.info(String.format("%x received %s rejection from %x at term %d", this.id, t, id, Term));
        }

        votes.putIfAbsent(id, v);

        int granted = 0;
        for (boolean vv : votes.values()) {
            if (vv) granted++;
        }
        return granted;
    }


    public void Step(pb.Message m) {
        // Handle the message term, which may result in our stepping down to a follower.
        {
            // local message?
            if (m.Term == 0L) {
                // todo
            }
            if (m.Term > Term) {
                // Never change our term in response to a PreVote
                // We send pre-vote requests with a term in our future. If the
                // pre-vote is granted, we will increment our term when we get a
                // quorum. If it is not, the term comes from the node that
                // rejected our vote so we should become a follower at the new
                // term.
                if (m.Type == MessageType.MsgPreVote || (m.Type == MessageType.MsgPreVoteResp && !m.Reject)) {
                } else {
                    logger.info(String.format("%x [term: %d] received a %s message with higher term from %x [term: %d]", id, Term, m.Type, m.From, m.Term));
                    if (m.Type == MessageType.MsgApp || m.Type == MessageType.MsgHeartbeat || m.Type == MessageType.MsgSnap) {
                        becomeFollower(m.Term, m.From);
                    } else {
                        // there is no lead yet
                        becomeFollower(m.Term, 0L);
                    }
                }
            } else if (m.Term < Term) {
                if ((checkQuorum || preVote) && (m.Type == MessageType.MsgHeartbeat || m.Type == MessageType.MsgApp)) {
                    // We have received messages from a leader at a lower term. It is possible
                    // that these messages were simply delayed in the network, but this could
                    // also mean that this node has advanced its term number during a network
                    // partition, and it is now unable to either win an election or to rejoin
                    // the majority on the old term. If checkQuorum is false, this will be
                    // handled by incrementing term numbers in response to MsgVote with a
                    // higher term, but if checkQuorum is true we may not advance the term on
                    // MsgVote and must generate other messages to advance the term. The net
                    // result of these two features is to minimize the disruption caused by
                    // nodes that have been removed from the cluster's configuration: a
                    // removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
                    // but it will not receive MsgApp or MsgHeartbeat, so it will not create
                    // disruptive term increases, by notifying leader of this node's activeness.
                    // The above comments also true for Pre-Vote
                    //
                    // When follower gets isolated, it soon starts an election ending
                    // up with a higher term than leader, although it won't receive enough
                    // votes to win the election. When it regains connectivity, this response
                    // with "pb.MsgAppResp" of higher term would force leader to step down.
                    // However, this disruption is inevitable to free this stuck node with
                    // fresh election. This can be prevented with Pre-Vote phase.
                    send(pb.Message.builder().To(m.From).Type(MessageType.MsgAppResp).build());
                } else if (m.Type == MessageType.MsgPreVote) {
                    // Before Pre-Vote enable, there may have candidate with higher term,
                    // but less log. After update to Pre-Vote, the cluster may deadlock if
                    // we drop messages with a lower term.
                    logger.info(String.format("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
                            id, raftLog.lastTerm(), raftLog.lastIndex(), Vote, m.Type, m.From, m.LogTerm, m.Index, Term));
                    send(pb.Message.builder().To(m.From).Term(Term).Type(MessageType.MsgPreVoteResp).Reject(true).build());
                } else {
                    // ignore other cases
                    logger.info(String.format("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
                            id, Term, m.Type, m.From, m.Term));
                }
                return;
            }
        }

        // Handle message type
        if (m.Type == MessageType.MsgHup) {
            if (state != StateType.StateLeader) {
                // Not leader -> MsgHup -> campaign
                Entry[] ents = raftLog.slice(raftLog.applied + 1, raftLog.committed + 1, Long.MAX_VALUE);

                int n = numOfPendingConf(ents);
                if (n != 0 && raftLog.committed > raftLog.applied) {
                    logger.warning(String.format("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", this.id, this.Term, n));
                    return;
                }

                logger.info(String.format("%x is starting a new election at term %d", id, Term));
                if (preVote) {
                    campaign(CampaignType.campaignPreElection);
                } else {
                    campaign(CampaignType.campaignElection);
                }
            } else {
                logger.debug(String.format("%x ignoring MsgHup because already leader", id));
            }
        }

        else if (m.Type == MessageType.MsgVote || m.Type == MessageType.MsgPreVote) {
            if (isLearner) {
                // TODO learner may need to vote, in case of node down when confchange.
                logger.info(String.format("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: learner can not vote",
                        id, raftLog.lastTerm(), raftLog.lastIndex(), Vote, m.Type, m.From, m.LogTerm, m.Index, Term));
                return;
            }
            // We can vote if this is a repeat of a vote we've already cast...
            boolean canVote = Vote == m.From ||
                    // ...we haven't voted and we don't think there's a leader yet in this term...
                    (Vote == 0L && lead == 0L) ||
                    // ...or this is a PreVote for a future term...
                    (m.Type == MessageType.MsgPreVote && m.Term > Term);
            // ...and we believe the candidate is up to date.
            if (canVote && raftLog.isUpToDate(m.Index, m.LogTerm)) {
                logger.info(String.format("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
                        id, raftLog.lastTerm(), raftLog.lastIndex(), Vote, m.Type, m.From, m.LogTerm, m.Index, Term));
                // When responding to Msg{Pre,}Vote messages we include the term
                // from the message, not the local term. To see why consider the
                // case where a single node was previously partitioned away and
                // it's local term is now of date. If we include the local term
                // (recall that for pre-votes we don't update the local term), the
                // (pre-)campaigning node on the other end will proceed to ignore
                // the message (it ignores all out of date messages).
                // The term in the original message and current local term are the
                // same in the case of regular votes, but different for pre-votes.
                send(pb.Message.builder().To(m.From).Term(m.Term).Type(Util.voteRespMsgType(m.Type)).build());
                if (m.Type == MessageType.MsgVote) {
                    // Only record real votes.
                    electionElapsed = 0;
                    Vote = m.From;
                }

            } else {
                logger.info(String.format("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
                        id, raftLog.lastTerm(), raftLog.lastIndex(), Vote, m.Type, m.From, m.LogTerm, m.Index, Term));
                send(pb.Message.builder().To(m.From).Term(Term).Type(Util.voteRespMsgType(m.Type)).Reject(true).build());
            }
        }

        else {
            step.step(this, m);
        }

    }

    // promotable indicates whether state machine can be promoted to leader,
    // which is true when its own id is in progress list.
    public boolean promotable() {
        return prs.get(id) != null;
    }

    public void addNode(long id) {
        addNodeOrLearnerNode(id, false);
    }

    public void addLearner(long id) {
        addNodeOrLearnerNode(id, true);
    }

    private void addNodeOrLearnerNode(long id, boolean isLearner) {
        Progress pr = getProgress(id);
        if (pr == null) {
            setProgress(id, 0L, raftLog.lastIndex() + 1, isLearner);
        } else {
            if (isLearner && !pr.isLearner) {
                // can only change Learner to Voter
                logger.info(String.format("%x ignored addLearner: do not support changing %x from raft peer to learner.", this.id, id));
                return;
            }

            if (isLearner == pr.isLearner) {
                // Ignore any redundant addNode calls (which can happen because the
                // initial bootstrapping entries are applied twice).
                return;
            }

            // change Learner to Voter, use origin Learner progress
            learnerPrs.remove(id);
            pr.isLearner = false;
            prs.put(id, pr);
        }

        if (this.id == id) {
            this.isLearner = isLearner;
        }

        // When a node is first added, we should mark it as recently active.
        // Otherwise, CheckQuorum may cause us to step down if it is invoked
        // before the added node has a chance to communicate with us.
        pr = getProgress(id);
        pr.RecentActive = true;
    }

    private void setProgress(long id, long match, long next, boolean isLearner) {
        if (!isLearner) {
            learnerPrs.remove(id);
            prs.put(id, Progress.builder().Next(next).Match(match).ins(Inflights.newInflights(maxInflight)).build());
            return;
        }
        if (!prs.containsKey(id)) {
            Panic.panic(String.format("%x unexpected changing from voter to learner for %x", this.id, id));
        }
        learnerPrs.put(id, Progress.builder().Next(next).Match(match).ins(Inflights.newInflights(maxInflight)).isLearner(true).build());
    }

    // pastElectionTimeout returns true if r.electionElapsed is greater
    // than or equal to the randomized election timeout in
    // [electiontimeout, 2 * electiontimeout - 1].
    public boolean pastElectionTimeout() {
        return this.electionElapsed >= this.randomizedElectionTimeout;
    }

    // checkQuorumActive returns true if the quorum is active from
    // the view of the local raft state machine. Otherwise, it returns
    // false.
    // checkQuorumActive also resets all RecentActive to false.
    public boolean checkQuorumActive() {
        // TODO
        return false;
    }

    public void sendTimeoutNow(Long to) {
        this.send(pb.Message.builder().To(to).Type(MsgTimeoutNow).build());
    }

    static int numOfPendingConf(pb.Entry[] ents) {
        if (ents == null) return 0;
        int n = 0;
        for (int i = 0; i < ents.length; i++) {
            if (ents[i].Type == EntryType.EntryConfChange) { n++; }
        }
        return n;
    }

}

interface stepFunc {
    void step(Raft r, pb.Message m);
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
class stepCandidate implements stepFunc {

    // Only handle vote responses corresponding to our candidacy (while in
    // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
    // our pre-candidate state).
    @Override
    public void step(Raft r, Message m) {

        MessageType myVoteRespType;
        if (r.state == StateType.StatePreCandidate) {
            myVoteRespType = MessageType.MsgPreVoteResp;
        } else {
            myVoteRespType = MessageType.MsgVoteResp;
        }

        switch (m.Type) {
            case MsgProp:
                r.logger.info(String.format("%x no leader at term %d; dropping proposal", r.id, r.Term));
                return;
            case MsgApp:
                r.becomeFollower(m.Term, m.From); // always m.Term == r.Term
                r.handleAppendEntries(m);
            case MsgHeartbeat:
                r.becomeFollower(m.Term, m.From); // always m.Term == r.Term
                r.handleHeartbeat(m);
            case MsgSnap:
                r.becomeFollower(m.Term, m.From);
                r.handleSnapshot(m);
            case MsgPreVoteResp:
            case MsgVoteResp:
                if (m.Type == myVoteRespType) {
                    int gr = r.poll(m.From, m.Type, !m.Reject);
                    r.logger.info(String.format("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.Type, r.votes.size() - gr));

                    // if become leader
                    int q = r.quorum();
                    if (q == gr) {
                        if (r.state == StateType.StatePreCandidate) {
                            r.campaign(CampaignType.campaignElection);
                        } else {
                            r.becomeLeader();
                            r.bcastAppend();
                        }
                    } else if (q == r.votes.size() - gr) {
                        // pb.MsgPreVoteResp contains future term of pre-candidate
                        // m.Term > r.Term; reuse r.Term
                        r.becomeFollower(r.Term, 0L);
                    }
                }
            case MsgTimeoutNow:
                r.logger.debug(String.format("%x [term %d state %s] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From));
        }

    }
}

class stepFollower implements stepFunc {
    @Override
    public void step(Raft r, Message m) {
        switch (m.Type) {
            case MsgProp:
                if (r.lead == 0L) {
                    r.logger.info(String.format("%x no leader at term %d; dropping proposal", r.id, r.Term));
                    return;
                } else if (r.disableProposalForwarding) {
                    r.logger.info(String.format("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term));
                    return;
                }
                m.To = r.lead;
                r.send(m);

            // handle messages from leader
            case MsgApp:
                // reset timer
                r.electionElapsed = 0;
                r.lead = m.From;
                r.handleAppendEntries(m);
            case MsgHeartbeat:
                r.electionElapsed = 0;
                r.lead = m.From;
                r.handleHeartbeat(m);
            case MsgSnap:
            case MsgTransferLeader:
            case MsgTimeoutNow:
            case MsgReadIndex:


        }
    }
}

// Leader has a lot to deal with
class stepLeader implements stepFunc {
    @Override
    public void step(Raft r, Message m) {
        // These message types do not require any progress for m.From.
        switch (m.Type) {
            case MsgBeat:
                r.bcastHeartbeat();
                return;
            case MsgCheckQuorum:
                if (!r.checkQuorumActive()) {
                    r.logger.warning(String.format("%x stepped down to follower since quorum is not active", r.id));
                    r.becomeFollower(r.Term, 0L);
                }
                return;
            case MsgProp:
                if (m.Entries.length == 0) {
                    Panic.panic(String.format("%x stepped empty MsgProp", r.id));
                }
                if (r.prs.get(r.id) == null) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    return;
                }
                if (r.leadTransferee != 0L) {
                    r.logger.debug(String.format("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee));
                    return;
                }

                // conf change
                for (int i = 0; i < m.Entries.length; i++) {
                    if (m.Entries[i].Type == EntryType.EntryConfChange) {
                        if (r.pendingConfIndex > r.raftLog.applied) {
                            r.logger.info(String.format("propose conf %s ignored since pending unapplied configuration [index %d, applied %d]",
                                    m.Entries[i].toString(), r.pendingConfIndex, r.raftLog.applied));
                            m.Entries[i] = pb.Entry.builder().Type(EntryType.EntryNormal).build();
                        } else {
                            r.pendingConfIndex = r.raftLog.lastIndex() + i + 1;
                        }
                    }
                }
                r.appendEntry(m.Entries);
                r.bcastAppend();
                return;
            case MsgReadIndex:
                if (r.quorum() > 1) {
                    if (r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term) {
                        // Reject read only request when this leader has not committed any log entry at its term.
                        return;
                    }

                    // thinking: use an internally defined context instead of the user given context.
                    // We can express this in terms of the term and index instead of a user-supplied value.
                    // This would allow multiple reads to piggyback on the same message.
                    switch (r.readOnly.option) {
                        case ReadOnlySafe:
                            // TODO
                    }
                } else {}
                return;
        }

        // All other message types require a progress for m.From (pr).
        Progress pr = r.getProgress(m.From);
        if (pr == null) {
            r.logger.debug(String.format("%x no progress available for %x", r.id, m.From));
            return;
        }
        switch (m.Type) {
            case MsgAppResp:
                pr.RecentActive = true;

                // if rejected???
                if (m.Reject) {
                    r.logger.debug(String.format("%x received msgApp rejection(lastindex: %d) from %x for index %d",
                            r.id, m.RejectHint, m.From, m.Index));
                    if (pr.maybeDecrTo(m.Index, m.RejectHint)) {
                        r.logger.debug(String.format("%x decreased progress of %x to [%s]", r.id, m.From, pr));
                        if (pr.State == ProgressStateReplicate) {
                            pr.becomeProbe();
                        }
                        r.sendAppend(m.From);
                    }
                } else {
                    boolean oldPaused = pr.IsPaused();
                    if (pr.maybeUpdate(m.Index)) {
                        switch (pr.State) {
                            case ProgressStateProbe:
                                pr.becomeReplicate();
                            case ProgressStateSnapshot:
                                if (pr.needSnapshotAbort()) {
                                    r.logger.debug(String.format("%x snapshot aborted, resumed sending replication messages to %x [%s]", r.id, m.From, pr));
                                    pr.becomeProbe();
                                }
                            case ProgressStateReplicate:
                                pr.ins.freeTo(m.Index);
                        }

                        if (r.maybeCommit()) {
                            r.bcastAppend();
                        } else if (oldPaused) {
                            // update() reset the wait state on this node. If we had delayed sending
                            // an update before, send it now.
                            r.sendAppend(m.From);
                        }
                        // Transfer leadership is in progress.
                        if ( m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex()) {
                            r.logger.info(String.format("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From));
                            r.sendTimeoutNow(m.From);
                        }
                    }
                }
            case MsgHeartbeatResp:
                pr.RecentActive = true;
                pr.resume();

                // free one slot for the full inflights window to allow progress.
                if (pr.State == ProgressStateReplicate && pr.ins.full()) {
                    pr.ins.freeFirstOne();
                }
                if (pr.Match < r.raftLog.lastIndex()) {
                    r.sendAppend(m.From);
                }

                if (r.readOnly.option != ReadOnlySafe || m.Context.length == 0) {
                    return;
                }

                int ackCount = r.readOnly.recvAck(m);
                if (ackCount < r.quorum()) {
                    return;
                }

                // TODO rss

            case MsgSnapStatus:
                // TODO
            case MsgUnreachable:
                // TODO
            case MsgTransferLeader:
                // TODO

        }
    }
}

interface tick {
    void tick(Raft r);
}

// tickElection is run by followers and candidates after r.electionTimeout.
class tickElection implements tick {
    @Override
    public void tick(Raft r) {
        r.electionElapsed++;
        LOG.debug("Raft tickElection...electionElapsed " + r.electionElapsed + " randomizedElectionTimeout " + r.randomizedElectionTimeout);
        if (r.promotable() && r.pastElectionTimeout()) {
            LOG.debug("Raft tickElection passed");
            r.electionElapsed = 0;
            // which Term???
            r.Step(pb.Message.builder().From(r.id).Term(r.Term).Type(MessageType.MsgHup).build());
        }
    }
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
class tickHeartbeat implements tick {
    @Override
    public void tick(Raft r) {
        LOG.debug(r.id + " tick heartbeat heartbeatElapsed " + r.heartbeatElapsed
                + " electionElapsed " + r.electionElapsed + " heartbeatTimeout " + r.heartbeatTimeout + " electionTimeout " + r.electionTimeout);
        r.heartbeatElapsed++;
        r.electionElapsed++;

        if (r.electionElapsed >= r.electionTimeout) {
            r.electionElapsed = 0;
            if (r.checkQuorum) {
                r.Step(Message.builder().From(r.id).Type(MsgCheckQuorum).build());
            }
            // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
            if (r.state == StateType.StateLeader && r.leadTransferee != 0L) {
                r.abortLeaderTransfer();
            }
        }

        if (r.state != StateType.StateLeader) {
            return;
        }

        if (r.heartbeatElapsed >= r.heartbeatTimeout) {
            r.heartbeatElapsed = 0;
            // set term ???
            r.Step(Message.builder().From(r.id).Type(MsgBeat).Term(r.Term).build());
        }
    }
}





package pb;

/**
 * Created by chengwenjie on 2018/5/29.
 */
public enum MessageType {
    MsgHup,
    MsgBeat,
    MsgProp,
    MsgApp,
    MsgAppResp,
    MsgVote,
    MsgVoteResp,
    MsgSnap,
    MsgHeartbeat,
    MsgHeartbeatResp,
    MsgUnreachable,
    MsgSnapStatus,
    MsgCheckQuorum,
    MsgTransferLeader,
    MsgTimeoutNow,
    MsgReadIndex,
    MsgReadIndexResp,
    MsgPreVote,
    MsgPreVoteResp
}

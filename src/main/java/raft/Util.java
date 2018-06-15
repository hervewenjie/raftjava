package raft;

import pb.MessageType;
import util.Panic;

/**
 * Created by chengwenjie on 2018/6/5.
 */
public class Util {

    public static pb.MessageType voteRespMsgType(pb.MessageType msgt) {
        switch (msgt) {
            case MsgVote:
                return MessageType.MsgVoteResp;
            case MsgPreVote:
                return MessageType.MsgPreVoteResp;
            default:
                Panic.panic(String.format("not a vote message: %s", msgt));
        }
        return null;
    }

    public static boolean IsResponseMsg(pb.MessageType msgt) {
        return msgt == MessageType.MsgAppResp || msgt == MessageType.MsgVoteResp || msgt == MessageType.MsgHeartbeatResp || msgt == MessageType.MsgUnreachable || msgt == MessageType.MsgPreVoteResp;
    }
}

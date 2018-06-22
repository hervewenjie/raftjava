package raft;

import pb.Entry;
import pb.MessageType;
import util.Panic;

import java.util.Arrays;

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

    public static Entry[] limitSize(Entry[] ents, long maxSize) {
        if (ents == null || ents.length == 0) {
            return ents;
        }
        long size = ents[0].Size();
        int limit;
        for (limit = 1; limit < ents.length; limit++) {
            size += ents[limit].Size();
            if (size > maxSize) {
                break;
            }
        }
        return Arrays.copyOfRange(ents, 0, limit);
    }
}

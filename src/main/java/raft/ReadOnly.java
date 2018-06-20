package raft;

import lombok.Builder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by chengwenjie on 2018/5/30.
 */
@Builder
public class ReadOnly {

    ReadOnlyOption option;
    Map<String, ReadIndexStatus> pendingReadIndex;
    String[] readIndexQueue;

    public static ReadOnly newReadOnly(ReadOnlyOption readOnlyOption) {
        return ReadOnly.builder().option(readOnlyOption).pendingReadIndex(new HashMap<>()).build();
    }

    public int recvAck(pb.Message m) {
        ReadIndexStatus rs = pendingReadIndex.get(new String(m.Context));
        if (rs == null) return 0;

        rs.acks.put(m.From, new Object());
        // add one to include an ack from local node
        return rs.acks.size() + 1;
    }

    // advance advances the read only request queue kept by the readonly struct.
    // It dequeues the requests until it finds the read only request that has
    // the same context as the given `m`.
    public ReadIndexStatus[] advance() {
        return null;
    }

    // lastPendingRequestCtx returns the context of the last pending read only
    // request in readonly struct.
    public String lastPendingRequestCtx() {
        if (readIndexQueue == null || readIndexQueue.length == 0) {
            return "";
        }
        return readIndexQueue[readIndexQueue.length - 1];
    }
}

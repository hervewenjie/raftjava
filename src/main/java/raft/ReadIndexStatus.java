package raft;

import java.util.Map;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public class ReadIndexStatus {
    pb.Message req;
    Long index;
    Map<Long, Object> acks;
}

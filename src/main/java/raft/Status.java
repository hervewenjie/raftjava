package raft;

import java.util.Map;

/**
 * Created by chengwenjie on 2018/6/4.
 */
public class Status {

    Long ID;

    pb.HardState hardState;
    SoftState    softState;

    Long Applied;
    Map<Long, Progress> Progress;

    Long LeadTransferee;

}

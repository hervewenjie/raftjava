package raft;

import lombok.Builder;

/**
 * SoftState provides state that is useful for logging and debugging.
 * The state is volatile and does not need to be persisted to the WAL.
 *
 * Created by chengwenjie on 2018/6/4.
 */
@Builder
public class SoftState {
    Long Lead;
    StateType RaftState;
}

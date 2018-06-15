package raft;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public enum StateType {
    StateFollower,
    StateCandidate,
    StateLeader,
    StatePreCandidate
}

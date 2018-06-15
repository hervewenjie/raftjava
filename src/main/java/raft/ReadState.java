package raft;

/**
 * ReadState provides state for read only query.
 * It's caller's responsibility to call ReadIndex first before getting
 * this state from ready, it's also caller's duty to differentiate if this
 * state is what it requests through RequestCtx, eg. given a unique id as
 * RequestCtx
 *
 * Created by chengwenjie on 2018/5/30.
 */
public class ReadState {
    Long index;
    byte[] requestCtx;
}

package raft;

import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * Created by chengwenjie on 2018/6/1.
 */
@Builder
@AllArgsConstructor
public class Peer {
    Long ID;
    byte[] Context;
}

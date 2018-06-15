package raft;

import java.util.Map;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public interface ProgressHandler {
    void handle(Long id, Map<Long, Progress> m);
}

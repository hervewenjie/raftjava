package example;

import lombok.Builder;
import raft.Logger;

/**
 * Created by chengwenjie on 2018/5/30.
 */
@Builder
public class SnapShotter {
    Logger lg;
    String dir;
}

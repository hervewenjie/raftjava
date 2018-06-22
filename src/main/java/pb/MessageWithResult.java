package pb;

import lombok.Builder;

import java.util.concurrent.BlockingQueue;

/**
 * Created by chengwenjie on 2018/6/1.
 */
@Builder
public class MessageWithResult {
    public pb.Message               m;
    // should be error here
    public BlockingQueue<Object> result;
}

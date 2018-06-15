package pb;

import lombok.Builder;

import java.util.Queue;

/**
 * Created by chengwenjie on 2018/6/1.
 */
@Builder
public class MessageWithResult {
    public pb.Message               m;
    public Queue<MessageWithResult> result;
}

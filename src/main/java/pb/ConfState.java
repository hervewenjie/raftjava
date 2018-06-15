package pb;

/**
 * Created by chengwenjie on 2018/5/29.
 */
public class ConfState {
    public long[] nodes = {};    // `protobuf:"varint,1,rep,name=nodes" json:"nodes,omitempty"`
    public long[] learners = {}; // `protobuf:"varint,2,rep,name=learners" json:"learners,omitempty"`
    public byte[] XXX_unrecognized; // `json:"-"`
}

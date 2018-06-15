package pb;

/**
 * Created by chengwenjie on 2018/5/29.
 */
public class SnapshotMetadata {
    public ConfState   ConfState;  // `protobuf:"bytes,1,opt,name=conf_state,json=confState" json:"conf_state"`
    public long        Index;      // `protobuf:"varint,2,opt,name=index" json:"index"`
    public long        Term;       // `protobuf:"varint,3,opt,name=term" json:"term"`
    public byte[]      XXX_unrecognized; // `json:"-"`
}

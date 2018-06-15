package pb;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public class HardState {
    public long Term;                // `protobuf:"varint,1,opt,name=term" json:"term"`
    public long Vote;                // `protobuf:"varint,2,opt,name=vote" json:"vote"`
    public long Commit;              // `protobuf:"varint,3,opt,name=commit" json:"commit"`
    public byte[] XXX_unrecognized;  // `json:"-"`
}

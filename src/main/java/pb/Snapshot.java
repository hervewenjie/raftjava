package pb;

/**
 * Created by chengwenjie on 2018/5/29.
 */
public class Snapshot {
    public byte[] data;               // `protobuf:"bytes,1,opt,name=data" json:"data,omitempty"`
    public SnapshotMetadata Metadata = new SnapshotMetadata(); // `protobuf:"bytes,2,opt,name=metadata"json:"metadata"`
    public byte[] XXX_unrecognized;
}

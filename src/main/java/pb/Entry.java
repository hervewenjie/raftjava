package pb;

import lombok.Builder;

/**
 * Created by chengwenjie on 2018/5/29.
 */
@Builder
public class Entry implements Marshaler {
    public long Term;         //    `protobuf:"varint,2,opt,name=Term" json:"Term"`
    public long Index;        //    `protobuf:"varint,3,opt,name=Index" json:"Index"`
    public EntryType Type;    //    `protobuf:"varint,1,opt,name=Type,enum=raftpb.EntryType" json:"Type"`
    public byte[] Data;       //    `protobuf:"bytes,4,opt,name=Data" json:"Data,omitempty"`
    public byte[] XXX_unrecognized; //  `json:"-"`

    @Override
    public byte[] Marshal() {
        return new byte[0];
    }
}

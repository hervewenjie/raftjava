package pb;

import lombok.Builder;

/**
 * Created by chengwenjie on 2018/5/30.
 */
@Builder
public class ConfChange {

    Long ID;                   // `protobuf:"varint,1,opt,name=ID" json:"ID"`
    ConfChangeType type;       //  `protobuf:"varint,2,opt,name=Type,enum=raftpb.ConfChangeType" json:"Type"`
    Long NodeID;               //  `protobuf:"varint,3,opt,name=NodeID" json:"NodeID"`
    byte[] Context;            //  `protobuf:"bytes,4,opt,name=Context" json:"Context,omitempty"`
    byte[] XXX_unrecognized;   //  `json:"-"`

    public byte[] Marshal() {
        int size = this.size();
        byte[] dAtA = new byte[size];

        return dAtA;
    }

    int size() {
        return 0;
    }
}

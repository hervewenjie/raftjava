package pb;

import lombok.Builder;

import java.io.Serializable;

/**
 * Created by chengwenjie on 2018/5/29.
 */
@Builder
public class Message implements Serializable {
    public MessageType       Type;       // `protobuf:"varint,1,opt,name=type,enum=raftpb.MessageType" json:"type"`
    public long              To;         // `protobuf:"varint,2,opt,name=to" json:"to"`
    public long              From;       // `protobuf:"varint,3,opt,name=from" json:"from"`
    public long              Term;       // `protobuf:"varint,4,opt,name=term" json:"term"`
    public long              LogTerm;    // `protobuf:"varint,5,opt,name=logTerm" json:"logTerm"`
    public long              Index;      // `protobuf:"varint,6,opt,name=index" json:"index"`
    public Entry[]           Entries;    // `protobuf:"bytes,7,rep,name=entries" json:"entries"`
    public long              Commit;     // `protobuf:"varint,8,opt,name=commit" json:"commit"`
    public Snapshot          Snapshot;   // `protobuf:"bytes,9,opt,name=snapshot" json:"snapshot"`
    public boolean           Reject;     // `protobuf:"varint,10,opt,name=reject" json:"reject"`
    public long              RejectHint; // `protobuf:"varint,11,opt,name=rejectHint" json:"rejectHint"`
    public byte[]            Context;    // `protobuf:"bytes,12,opt,name=context" json:"context,omitempty"`
    public byte[]            XXX_unrecognized; //`json:"-"`
}

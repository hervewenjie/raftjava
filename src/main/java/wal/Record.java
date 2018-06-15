package wal;

import lombok.Builder;

/**
 * Created by chengwenjie on 2018/6/14.
 */
@Builder
public class Record {
    long Type;       //  `protobuf:"varint,1,opt,name=type" json:"type"`
    int Crc;         //  `protobuf:"varint,2,opt,name=crc" json:"crc"`
    byte[] Data;     // `protobuf:"bytes,3,opt,name=data" json:"data,omitempty"`
    byte[] XXX_unrecognized; //  `json:"-"`
}

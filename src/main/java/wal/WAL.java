package wal;

import pb.Entry;
import pb.HardState;
import pb.PbUtil;
import pb.Snapshot;
import raft.Logger;
import raft.Raft;
import raft.Ready;

import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WAL is a logical representation of the stable storage.
 * WAL is either in read mode or append mode but not both.
 * A newly created WAL is in append mode, and ready for appending records.
 * A just opened WAL is in read mode, and ready for reading records.
 * The WAL will be ready for appending after reading out all the previous records.
 *
 * Created by chengwenjie on 2018/5/31.
 */
public class WAL {

    Logger lg;

    String dir; // the living directory of the underlay files

    // dirFile is a fd for the wal directory for syncing on Rename
    File dirFile;

    byte[] metadata;         // metadata recorded at the head of each WAL
    pb.HardState state ;     // hardstate recorded at the head of WAL

    Snapshot start;          // snapshot to start reading
//    decoder   *decoder       // decoder to decode records
//    readClose func() error   // closer for decode reader

    Lock mu = new ReentrantLock();
    long enti;                  // index of the last entry saved to the wal
    Encoder encoder;            // encoder to encode records

//    Llocks []*fileutil.LockedFile // the locked files the WAL holds (the name is increasing)
//    fp    *filePipeline

    // Exist returns true if there are any files in a given directory.
    public static boolean exists(String dir) {
        File dirFile = new File(dir);
        String[] names = dirFile.list((f, n) -> n.endsWith(".wal"));
        if (names == null) { return false; }
        return names.length != 0;
    }

    public synchronized void Save(HardState st, Entry[] ents) {

        // short cut, do not call sync
        if (Ready.IsEmptyHardState(st) && (ents == null || ents.length == 0)) {
            return;
        }

        boolean mustSync = Ready.MustSync(st, state, ents.length);

        for (int i = 0; i < ents.length; i++) {

        }

    }

    public void saveEntry(Entry e) {
        byte[] b = PbUtil.MustMarshal(e);
        Record rec = Record.builder().Type(RecordType.entryType.ordinal()).Data(b).build();
        encoder.encode(rec);
        enti = e.Index;
    }

    public void ReadAll() {

    }
}

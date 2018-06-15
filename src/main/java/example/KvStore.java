package example;

import com.alibaba.fastjson.JSON;
import util.Encoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public class KvStore {

    Queue<String> proposeC;          // channel for proposing updates
    ReentrantLock mu = new ReentrantLock();
    Map<String, String> kvStore; // current committed key-value pairs
    SnapShotter snapshotter;

    public static KvStore newKvStore(SnapShotter snapShoter, Queue<String> proposeC, Queue<KV> commitC) {
        KvStore s = new KvStore();
        s.proposeC    = proposeC;
        s.kvStore     = new HashMap<>();
        s.snapshotter = snapShoter;
        // replay log into key-value map
        s.readCommits(commitC);

        // read commits from raft into kvStore map until error
        new Thread(() -> s.readCommits(commitC)).start();

        return s;
    }

    public void propose(String k, String v) {
        proposeC.add(new String(Encoder.encode(new KV(k, v))));
    }

    public String lookup(String k) {
        mu.lock();
        String v = kvStore.get(k);
        mu.unlock();
        return v;
    }

    public void readCommits(Queue<KV> commitC) {
        for (KV kv : commitC) {
            if (kv == null) {
                // ...
            }
            mu.lock();
            kvStore.put(kv.key, kv.val);
            mu.unlock();
        }
    }

    public byte[] getSnapshot() {
        mu.lock();
        byte[] b = JSON.toJSONBytes(kvStore);
        mu.unlock();
        return b;
    }

    public void recoverFromSnapshot(byte[] snapshot) {
        Map<String, String> store = (Map<String, String>) JSON.parse(snapshot);
        mu.lock();
        this.kvStore = store;
        mu.unlock();
    }

    public static void main(String[] args) {
        Map<String, String> m = new HashMap<>();
        m.put("foo", "bar");
        KvStore s = new KvStore(); s.kvStore = m;

        System.out.println(s.lookup("foo"));

        byte[] snapshot = s.getSnapshot();
        System.out.println(snapshot);

        s.kvStore = null;

        s.recoverFromSnapshot(snapshot);

        System.out.println("After recovery");
        System.out.println(s.lookup("foo"));
    }
}
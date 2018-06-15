package example;

import javafx.util.Pair;
import pb.ConfChange;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by chengwenjie on 2018/5/31.
 */
public class Main1 {

    // curl -XPUT -d "iamvalue" http://localhost:9121/iamkey

    public static void main(String[] args) {
        String   cluster = "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023"; // comma separated cluster peers
        int      id      = 1;
        int      kvport  = 9121;
        boolean  join    = false;

        Queue<String> proposeC    = new ArrayBlockingQueue<>(128);
        Queue<ConfChange> confChangeC = new ArrayBlockingQueue<>(128);

        // raft provides a commit stream for the proposals from the http api
        KvStore kvs;
        SnapshotFunc snapshotFunc = new SnapshotFuncImpl();
        // new raft node
        Pair<Queue<KV>, Queue<SnapShotter>> p =
                RaftNode.newRaftNode(id, cluster.split(","), join, snapshotFunc, proposeC, confChangeC);

        kvs = KvStore.newKvStore(p.getValue().poll(), proposeC, p.getKey());

        // the key-value http handler will propose updates to raft
        new HttpApi(kvs, confChangeC).serveHttpKVAPI(kvs, kvport, confChangeC);
    }
}

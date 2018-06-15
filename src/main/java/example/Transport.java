package example;

import com.alibaba.fastjson.JSON;
import lombok.Builder;
import pb.Message;
import raft.Logger;
import raft.Peer;
import util.Encoder;
import util.LOG;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Transport implements Transporter interface. It provides the functionality
 * to send raft messages to peers, and receive raft messages from peers.
 * User should call Handler method to get a handler to serve requests
 * received from peerURLs.
 * User needs to call Start before calling other functions, and call
 * Stop when the Transport is no longer used.
 *
 * Created by chengwenjie on 2018/5/31.
 */
@Builder
public class Transport {

    static int[] serverPort = {0, 8000, 8002, 8004};
    static int[] clientPort = {0, 8001, 8003, 8005};

    Logger logger;

    long DialTimeout;   // maximum duration before timing out dial of the request
    // DialRetryFrequency defines the frequency of streamReader dial retrial attempts;
    // a distinct rate limiter is created per every peer (default value: 10 events/sec)
    double DialRetryFrequency;

    TLSInfo tLSInfo; // TLS information used when creating connection

    long ID;          // local member ID
    String[] URLs;    // local peer URLs
    long  ClusterID;  // raft cluster ID for request validation
    RaftNode     raft;    // raft state machine, to which the Transport forwards received messages and reports status
    SnapShotter snapshotter;
//    ServerStats *stats.ServerStats // used to record general transportation statistics
    // used to record transportation statistics with followers when
    // performing as leader in raft protocol
//    LeaderStats *stats.LeaderStats
    // ErrorC is used to report detected critical errors, e.g.,
    // the member has been permanently removed from the cluster
    // When an error is received from ErrorC, user should stop raft state
    // machine and thus stop the Transport.
    Queue ErrorC;

//    streamRt   http.RoundTripper // roundTripper used by streams
//    pipelineRt http.RoundTripper // roundTripper used by pipelines

    ReentrantLock mu;            // protect the remote and peer map
    Map<Long, Peer> remotes;     // remotes map that helps newly joined member to catch up
    Map<Long, Peer> peers;       // peers map

//    prober probing.Prober

    public void start(int id) {
        // server
        new Thread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(serverPort[id]);
                while (true) {
                    // block
                    Socket socket = serverSocket.accept();
                    ObjectInputStream is = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                    Object obj = is.readObject();
                    Message m = (Message)obj;
                    LOG.info("server " + id + " received " + JSON.toJSONString(m));
                }
            } catch (IOException | ClassNotFoundException e) {
                LOG.error("transport start failed...");
            }

        }).start();

        LOG.info("transport started...");
    }

    public void Send(Message[] msgs) {
        if (msgs == null || msgs.length == 0) {
            LOG.warn("transport sending empty messages");
        }
        for (int i = 0; i < msgs.length; i++) {
            Message m = msgs[i];
            if (m.To == 0) { continue; }
            send(m);
            LOG.debug("transport sending to " + m.To);
        }
    }

    private void send(Message m) {
        try {
            Socket socket = new Socket("127.0.0.1", serverPort[(int) m.To]);
            ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
            os.writeObject(m);
            os.close();
            socket.close();
        } catch (IOException e) {
            LOG.error("transport send from " + m.From + " to " + m.To + " failed");
        }
    }

}

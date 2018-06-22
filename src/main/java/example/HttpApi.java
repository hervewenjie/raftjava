package example;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import lombok.AllArgsConstructor;
import pb.ConfChange;
import util.LOG;
import util.Panic;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;

/**
 * Created by chengwenjie on 2018/5/30.
 */
@AllArgsConstructor
public class HttpApi {

    KvStore    store;
    Queue<ConfChange> confChange;

    public void serveHttpKVAPI(KvStore kv, int port, Queue<ConfChange> confChangeC) {

        InetSocketAddress addr = new InetSocketAddress(port);
        try {
            HttpServer server = HttpServer.create(addr, 0);
            server.createContext("/", (exchange) -> {
                String requestMethod = exchange.getRequestMethod();

                String key = exchange.getRequestURI().getPath().substring(1);
                // PUT
                if (requestMethod.equalsIgnoreCase("PUT")) {
                    String value   = getString(exchange.getRequestBody());
                    LOG.debug("new request:" + requestMethod + " " + key + " " + value);

                    // propose to store
                    store.propose(key, value);

                    // Optimistic-- no waiting for ack from raft. Value is not yet
                    // committed so a subsequent GET on the key may return old value
                }
                // GET
                else if (requestMethod.equalsIgnoreCase("GET")) {
                    exchange.getResponseBody().write(this.store.lookup(key).getBytes());
                    exchange.getResponseBody().close();
                }

                // conf change with POST and DELETE
                // POST
                else if (requestMethod.equalsIgnoreCase("POST")) {

                }
                // DELETE
                else if (requestMethod.equalsIgnoreCase("DELETE")) {
                    Headers responseHeaders = exchange.getResponseHeaders();
                    responseHeaders.set("Content-Type", "text/plain");
                    exchange.sendResponseHeaders(200, 0);

                    OutputStream responseBody = exchange.getResponseBody();

                    responseBody.write("hello".getBytes());
                    responseBody.close();
                }
            });
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            LOG.info("Server is listening on port " + port);
        } catch (IOException e) {
            Panic.panic("start http server failed");
        }
    }

    private static String getString(InputStream inputStream) throws IOException {
        byte[] bytes = new byte[inputStream.available()];
        inputStream.read(bytes);
        String str = new String(bytes);
        return str;
    }

    public static void main(String[] args) {
        new HttpApi(null, null).serveHttpKVAPI(new KvStore(), 8080, new ArrayBlockingQueue<>(1));
    }
}

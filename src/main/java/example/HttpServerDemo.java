package example;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import util.LOG;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public class HttpServerDemo {
    public static void main(String[] args) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(8080);
        HttpServer server = HttpServer.create(addr, 0);

        server.createContext("/", (exchange) -> {
            String requestMethod = exchange.getRequestMethod();
            LOG.debug("处理新请求:"+requestMethod);
            if (requestMethod.equalsIgnoreCase("GET")) {
                Headers responseHeaders = exchange.getResponseHeaders();
                responseHeaders.set("Content-Type", "text/plain");
                exchange.sendResponseHeaders(200, 0);

                OutputStream responseBody = exchange.getResponseBody();
                Headers requestHeaders = exchange.getRequestHeaders();
                Set<String> keySet = requestHeaders.keySet();
                Iterator<String> iter = keySet.iterator();
                while (iter.hasNext()) {
                    String key = iter.next();
                    List values = requestHeaders.get(key);
                    String s = key + " = " + values.toString() + "\n";
                    responseBody.write(s.getBytes());
                }
                responseBody.close();
            }
        });
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        LOG.info("Server is listening on port 8080");
    }
}



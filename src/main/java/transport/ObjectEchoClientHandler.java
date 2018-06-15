package transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import pb.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Handler implementation for the object echo client.  It initiates the
 * ping-pong traffic between the object echo client and server by sending the
 * first message to the server.
 */
public class ObjectEchoClientHandler extends ChannelInboundHandlerAdapter {

    public Queue<Message> msgs;

    ObjectEchoClientHandler(Queue<Message> msgs) {
        this.msgs = msgs;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // Send the first message if his handler is a client-side handler.
        while (true) {
            if (msgs.peek() != null) {
                ctx.writeAndFlush(msgs.poll());
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {}
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}

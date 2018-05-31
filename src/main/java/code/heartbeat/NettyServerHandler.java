package code.heartbeat;

import java.util.Date;

import code.heartbeat.dto.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;

/**
 *
 */
public class NettyServerHandler extends SimpleChannelInboundHandler<BaseMsg> {

    private int UNCONNECT_NUM_S = 0;

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            if(UNCONNECT_NUM_S >= 4) {
                System.err.println("client connect status is disconnect.");
                ctx.close();
                //此处当重启次数达到4次之后，关闭此链接后，清除服务端相关的记录信息
                return;
            }

            IdleStateEvent e = (IdleStateEvent) evt;
            switch (e.state()) {
                case WRITER_IDLE:
                    System.out.println("send ping to client---date=" + new Date());
                    PingMsg pingMsg=new PingMsg();
                    ctx.writeAndFlush(pingMsg);
                    UNCONNECT_NUM_S++;
                    System.err.println("writer_idle over. and UNCONNECT_NUM_S=" + UNCONNECT_NUM_S);
                    break;
                case READER_IDLE:
                    System.err.println("reader_idle over.");
                    UNCONNECT_NUM_S++;
                    //读取服务端消息超时时，直接断开该链接，并重新登录请求，建立通道
                case ALL_IDLE:
                    System.err.println("all_idle over.");
                    UNCONNECT_NUM_S++;
                    //读取服务端消息超时时，直接断开该链接，并重新登录请求，建立通道
                default:
                    break;
            }
        }
    }


    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.err.println("in channelInactive.");
        NettyChannelMap.remove((SocketChannel)ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        System.err.println("in here has an error.");
        NettyChannelMap.remove((SocketChannel)ctx.channel());
        super.exceptionCaught(ctx, cause);
        System.err.println("channel is exception over. (SocketChannel)ctx.channel()=" + (SocketChannel)ctx.channel());
    }

    protected void channelRead0(ChannelHandlerContext channelHandlerContext, BaseMsg baseMsg) throws Exception {
        if(MsgType.LOGIN.equals(baseMsg.getType())){
            LoginMsg loginMsg=(LoginMsg)baseMsg;
            if("lin".equals(loginMsg.getUserName())&&"alan".equals(loginMsg.getPassword())){
                //登录成功,把channel存到服务端的map中
                NettyChannelMap.add(loginMsg.getClientId(),(SocketChannel)channelHandlerContext.channel());
                System.out.println("client"+loginMsg.getClientId()+" 登录成功");
            }
        }else{
            if(NettyChannelMap.get(baseMsg.getClientId())==null){
                //说明未登录，或者连接断了，服务器向客户端发起登录请求，让客户端重新登录
                LoginMsg loginMsg=new LoginMsg();
                channelHandlerContext.channel().writeAndFlush(loginMsg);
            }
        }
        switch (baseMsg.getType()){
            case PING:{
                PingMsg pingMsg=(PingMsg)baseMsg;
                ReplyMsg replyPing=new ReplyMsg();
                ReplyServerBody body = new ReplyServerBody("send server msg.");
                replyPing.setBody(body);
                NettyChannelMap.get(pingMsg.getClientId()).writeAndFlush(replyPing);
                System.err.println("ping over.");
            }break;
            case ASK:{
                //收到客户端的请求
                AskMsg askMsg=(AskMsg)baseMsg;
                if("authToken".equals(askMsg.getParams().getAuth())){
                    ReplyServerBody replyBody=new ReplyServerBody("receive client askmsg:" + askMsg.getParams().getContent());
                    ReplyMsg replyMsg=new ReplyMsg();
                    replyMsg.setBody(replyBody);
                    NettyChannelMap.get(askMsg.getClientId()).writeAndFlush(replyMsg);
                }
            }break;
            case REPLY:{
                //收到客户端回复
                ReplyMsg replyMsg=(ReplyMsg)baseMsg;
                ReplyClientBody clientBody=(ReplyClientBody)replyMsg.getBody();
                UNCONNECT_NUM_S = 0;
                System.out.println("UNCONNECT_NUM_S=" + UNCONNECT_NUM_S +",receive client replymsg: "+clientBody.getClientInfo());
            }break;
            default:break;
        }
        ReferenceCountUtil.release(baseMsg);
    }
}

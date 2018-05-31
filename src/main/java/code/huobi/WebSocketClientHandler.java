package code.huobi;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
    private Logger log = Logger.getLogger(WebSocketClientHandler.class);
    ConcurrentHashMap<String,ChannelGroup> rooms=new ConcurrentHashMap();
    private static final ChannelGroup CHANNEL_GROUP = new DefaultChannelGroup("ChannelGroups", GlobalEventExecutor.INSTANCE);


    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;

    public WebSocketClientHandler(WebSocketClientHandshaker handshaker) {
        this.handshaker = handshaker;
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handshaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        System.out.println("WebSocket Client disconnected!");
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        Channel ch = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            try {
                handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                log.info("WebSocket Client connected!");
                handshakeFuture.setSuccess();
            } catch (WebSocketHandshakeException e) {
                log.info("WebSocket Client failed to connect");
                handshakeFuture.setFailure(e);
            }
            return;
        }

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException(
                    "Unexpected FullHttpResponse (getStatus=" + response.status() +
                            ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
         if (frame instanceof PongWebSocketFrame) {
            System.out.println("WebSocket Client received pong");
        } else if (frame instanceof CloseWebSocketFrame) {
            System.out.println("WebSocket Client received closing");
            ch.close();
        }else if (frame instanceof BinaryWebSocketFrame){
            BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
             byte[] temp = new byte[binaryFrame.content().readableBytes()];
             binaryFrame.content().readBytes(temp);
             temp = GZipUtils.decompress(temp);
             String str = new String(temp,"UTF-8");
             if(str.contains("ping")) {
                 //log.info("send:" + str.replace("ping", "pong"));
                 ch.writeAndFlush(new TextWebSocketFrame(str.replace("ping", "pong")));
             }
             JSONObject json = JSON.parseObject(str);
             try {
                 if (json.containsKey("tick")){
                     String ts= json.getString("ts");
                     String chStr= json.getString("ch");
                     String bi=StringUtils.substring(chStr,chStr.indexOf(".")+1,chStr.indexOf(".kline")).replace("usdt","");
                     String tick= json.getString("tick");
                     JSONObject json2=JSONObject.parseObject(tick);
                     String close=json2.getString("close");
                     Date date = new Date(Long.valueOf(ts));
                     SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                     //log.info("----:"+format.format(date)+"---"+bi+"----:"+close.substring(0,8));
                     System.out.println(format.format(date)+"---"+bi+"----:"+close.substring(0,8));
                 }else{
                     //log.info("receive:" + json.toJSONString());
                 }
             } catch (Exception e) {
                 e.printStackTrace();
             }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }

}


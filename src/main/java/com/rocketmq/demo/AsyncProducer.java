package com.rocketmq.demo;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 异步发送消息示例
 */
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException {
        //创建一个名为 my_group_name 的组
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("my_group_name");
        //指定namesrv服务地址，多个地址以 ; 隔开
        defaultMQProducer.setNamesrvAddr("192.168.32.137:9876");
        defaultMQProducer.start();
        for (int i = 0; i < 100; i++) {
            try{
                //创建一个名为 myTopic 的主题，名为 myTag 的tag，并将消息转换未 byte[] 数据
                Message message = new Message("myTopic","myTag",("Hello RocketMQ"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                defaultMQProducer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.printf("异步发送消息返回结果：%s %n",sendResult.getSendStatus());
                    }

                    @Override
                    public void onException(Throwable e) {
                        e.printStackTrace();
                    }
                });
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
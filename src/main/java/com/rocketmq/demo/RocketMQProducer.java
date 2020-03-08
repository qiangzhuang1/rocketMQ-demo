package com.rocketmq.demo;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 消息发送端示例
 */
public class RocketMQProducer {
    public static void main(String[] args) throws MQClientException {
        //创建一个名为 my_group_name 的组
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("my_group_name");
        //指定namesrv服务地址，多个地址以 ; 隔开
        defaultMQProducer.setNamesrvAddr("192.168.32.138:9876");
        defaultMQProducer.start();
        for (int i = 0; i < 5; i++) {
            try{
                //创建一个名为 myTopic 的主题，名为 myTag 的tag，并将消息转换未 byte[] 数据
                Message message = new Message("myTopic","myTag",("Hello RocketMQ"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult send = defaultMQProducer.send(message);
                //发送消息并回去发送结果
                System.out.println("发送消息返回结果:"+send);
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
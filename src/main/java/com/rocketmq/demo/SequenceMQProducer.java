package com.rocketmq.demo;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * 顺序发送消息示例
 */
public class SequenceMQProducer {
    public static void main(String[] args) throws MQClientException {
        //创建一个名为 my_sequence_name 的组
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("my_sequence_name");
        //指定namesrv服务地址，多个地址以 ; 隔开
        defaultMQProducer.setNamesrvAddr("192.168.32.138:9876");
        defaultMQProducer.start();
        for (int i = 0; i < 100; i++){
            try {
                int orderId = i % 10;
                //创建一个名为 mySequenceTopic 的主题，名为 mySequenceTag 的tag，并将消息转换未 byte[] 数据
                Message message = new Message("mySequenceTopic",
                        "mySequenceTag", ("Hello sequence RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult send = defaultMQProducer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object o) {
                        Integer id = (Integer) o;
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, orderId);
                System.out.printf("发送消息返回结果:%s%n", send);
                Thread.sleep(1000);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
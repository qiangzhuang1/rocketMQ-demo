package com.rocketmq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import java.util.List;

/**
 * 消息消费端示例
 */
public class RocketMQConsumer {
    public static void main(String[] args) throws MQClientException {
        //创建一个名为 my_consumer_group_name 的组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("my_consumer_group_name");
        //指定namesrv服务地址，多个地址以 ; 隔开
        consumer.setNamesrvAddr("192.168.32.138:9876");
        //设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
        //如果非第一次启动，那么按照上次消费的位置继续消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //订阅消息生产者中的 myTopic，* 代表消费该 topic 下所有的 tag，*表示不过滤，可以通过tag来过滤，比如:”myTag”
        consumer.subscribe("myTopic","*");
        /**
         * 注册消息监听回调
         * 有两种回调 MessageListenerConcurrently 为普通监听，MessageListenerOrderly 为顺序监听
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt me : msgs) {
                    System.out.printf("消费消息：%s %n",new String(me.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }
}
package com.rocketmq.demo.transact;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 事务型消息发送端示例
 */
public class TransactProducer {
    public static void main(String[] args) throws MQClientException {
        TransactionMQProducer transactionMQProducer = new TransactionMQProducer("tx_group_name");
        transactionMQProducer.setNamesrvAddr("192.168.32.138:9876");
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        transactionMQProducer.setExecutorService(executorService);
        transactionMQProducer.setTransactionListener(new TransactionListenerLocal());

        transactionMQProducer.start();
        for (int i = 0; i < 8; i++) {
            try{
                String id= UUID.randomUUID().toString();
                Message message = new Message("tx_topic", "tx_tag", id, ("Hello Transaction RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                TransactionSendResult transactionSendResult = transactionMQProducer.sendMessageInTransaction(message, id + "&" + i);
                System.out.printf("发送状态：%s %n",transactionSendResult.getLocalTransactionState());
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
package com.rocketmq.demo.transact;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionListenerLocal implements TransactionListener {
    private static final Map<String,Boolean> results=new ConcurrentHashMap<>();
    //执行本地事务
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("执行本地事务:" + arg.toString());
        String id = arg.toString();
        boolean rs = saveId(id);
        // 这个返回状态表示告诉 broker 这个事务消息是否被确认，允许给到 consumer 进行消费
        // LocalTransactionState.ROLLBACK_MESSAGE 回滚
        //LocalTransactionState.UNKNOW 未知（这个状态的会执行回查方法，提供给broker回调）
        return rs ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.UNKNOW;
    }

    //提供事务执行状态的回查方法，提供给broker回调
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        String id = msg.getKeys();
        System.out.println("执行事务状态的回查，id:"+id);
        boolean rs = Boolean.TRUE.equals(results.get(id));
        System.out.println("回调："+rs);
        return rs ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.ROLLBACK_MESSAGE;
    }
    private boolean saveId(String id) {
        boolean success = Math.abs(Objects.hashCode(id)) % 2 == 0;
        results.put(id, success);
        return success;
    }
}
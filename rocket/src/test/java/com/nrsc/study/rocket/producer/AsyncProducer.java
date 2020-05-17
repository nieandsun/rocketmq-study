package com.nrsc.study.rocket.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AsyncProducer {
    public static void main(
            String[] args) throws MQClientException, InterruptedException {
        //生产者实例化
        DefaultMQProducer producer = new DefaultMQProducer("async");
        //指定rocket服务器地址
        producer.setNamesrvAddr("localhost:9876");
        //启动实例
        producer.start();
        //发送异步失败时的重试次数(这里不重试)
        producer.setRetryTimesWhenSendAsyncFailed(0);

        int messageCount = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        for (int i = 0; i < messageCount; i++) {
            try {
                final int index = i;
                Message msg = new Message("TopicTest",
                        "TagC",
                        "OrderID" + index,
                        ("Hello world " + index).getBytes(RemotingHelper.DEFAULT_CHARSET));
                //生产者异步发送
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d OK %s %n", index, new String(msg.getBody()));
                    }

                    @Override
                    public void onException(Throwable e) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d Exception %s %n", index, e);
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //Thread.sleep(1000);
        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.shutdown();
    }
}

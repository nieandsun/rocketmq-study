package com.nrsc.study.rocket.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/***
 * 拉模式
 */
public class PullConsumer {
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("pullconsumer");
        consumer.setNamesrvAddr("localhost:9876");
        //consumer.setBrokerSuspendMaxTimeMillis(1000);

        System.out.println("ms:" + consumer.getBrokerSuspendMaxTimeMillis());
        consumer.start();

        //1.获取MessageQueues并遍历（一个Topic包括多个MessageQueue）
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("Topic-NRSC");
        for (MessageQueue mq : mqs) {
            System.out.println("queueID:" + mq.getQueueId());
            //获取偏移量
            long Offset = consumer.fetchConsumeOffset(mq, true);

            System.out.printf("Consume from the queue: %s%n", mq);
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult =
                            consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.printf("%s%n", pullResult);
                    //2.维护Offsetstore（这里存入一个Map）
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());

                    //3.根据不同的消息状态做不同的处理
                    switch (pullResult.getPullStatus()) {
                        case FOUND: //获取到消息
                            for (int i = 0; i < pullResult.getMsgFoundList().size(); i++) {
                                System.out.printf("%s%n", new String(pullResult.getMsgFoundList().get(i).getBody()));
                            }
                            break;
                        case NO_MATCHED_MSG: //没有匹配的消息
                            break;
                        case NO_NEW_MSG:  //没有新消息
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL: //非法偏移量
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        consumer.shutdown();
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }
}

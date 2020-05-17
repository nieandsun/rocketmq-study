package com.nrsc.study.rocket.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

@Slf4j
public class OneWayProducer {
    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name. --- 生产者组
        DefaultMQProducer producer = new DefaultMQProducer("nrsc-group");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //Launch the instance.
        producer.start();
        for (int i = 0; i < 5; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message(
                    "Topic-NRSC" /* Topic */,
                    "TagA-NRSC" /* Tag */,
                    ("Hello RocketMQ " +
                            i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            //Call send message to deliver message to one of brokers.
            producer.sendOneway(msg);
            log.info("send msg is {}", msg);

        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}

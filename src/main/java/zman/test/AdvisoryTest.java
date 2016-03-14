package zman.test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQMessage;


public class AdvisoryTest {


    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://10.1.21.251:61616");
        Connection connection = factory.createConnection();
        connection.start();

        final Session session = connection.createSession(false/*支持事务*/, Session.AUTO_ACKNOWLEDGE);

        //ActiveMQ.Advisory.MessageConsumed.Queue.ActiveMQ.Advisory.Consumer.Queue.com.netfinworks.paypw.risk.notify
        //ActiveMQ.Advisory.Consumer.Queue.com.netfinworks.paypw.risk.notify
        Destination queue = AdvisorySupport.getConsumerAdvisoryTopic(
                session.createQueue("com.netfinworks.paypw.risk.notify"));
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
               System.out.println(message);
               ActiveMQMessage consumedMessage = (ActiveMQMessage) ((ActiveMQMessage) message).getDataStructure();
               System.out.println("队列：[" + consumedMessage.getDestination() + "]，消息：[id="
                      + consumedMessage.getMessageId() + "]被成功接收。");
            }
        });

        subtest();

     }

    private static void subtest() throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://10.1.21.251:61616");
        Connection connection = factory.createConnection();
        connection.start();

        final Session session = connection.createSession(false/*支持事务*/, Session.AUTO_ACKNOWLEDGE);

        //ActiveMQ.Advisory.MessageConsumed.Queue.ActiveMQ.Advisory.Consumer.Queue.com.netfinworks.paypw.risk.notify
        //ActiveMQ.Advisory.Consumer.Queue.com.netfinworks.paypw.risk.notify
        //ActiveMQ.Advisory.Consumer.Queue.com.netfinworks.rms.rules.riskevent
        Destination queue = AdvisorySupport.getConsumerAdvisoryTopic(
                session.createQueue("com.netfinworks.rms.rules.riskevent"));
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
               System.out.println(message);
               ActiveMQMessage consumedMessage = (ActiveMQMessage) ((ActiveMQMessage) message).getDataStructure();
               System.out.println("队列：[" + consumedMessage.getDestination() + "]，消息：[id="
                      + consumedMessage.getMessageId() + "]被成功接收。");
               try {
                message.acknowledge();
            } catch (JMSException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            }
        });
    }

}

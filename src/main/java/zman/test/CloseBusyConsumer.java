package zman.test;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Close Busy Consumer Test referring to
 */
public class CloseBusyConsumer {

    public static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://10.1.21.187:2002");

        // Create a Connection
        Connection connection = connectionFactory.createConnection();
        connection.start();

        // Create a Session
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        Destination destination = session.createQueue("TEST.FOO");

        // Create a MessageProducer from the Session to the Topic or Queue
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        for (int i = 0; i < 20; i++) {
         // Create a messages
            String text = "Hello world! From: " + Thread.currentThread().getName() ;
            Message message = session.createTextMessage(text);
            // Tell the producer to send the message
            System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
            producer.send(message);
        }

        // consumer1
        final MessageConsumer consumer1 = session.createConsumer(destination);
        MessageListener listner = new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.err.println(message);
                    consumer1.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        };
        consumer1.setMessageListener(listner);

        //consumer2
        // Create a MessageConsumer from the Session to the Queue
        MessageConsumer consumer2 = session.createConsumer(destination);
        for (int i = 0; i < 20; i++) {
            Message message = consumer2.receive();
            System.out.println(message.toString());
            message.acknowledge();
        }

     // Clean up
        consumer2.close();
        producer.close();
        session.close();
        connection.close();

        System.exit(0);

    }

}

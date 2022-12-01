import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.Setter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Setter
public class WikimediaChangeHandler implements EventHandler {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    private KafkaProducer<String, String> kafkaProducer;
    private String topic;

    @Override
    public void onOpen() {
        // nothing to do here
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        logger.info("Sending event to Kafka: " + messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) {
        // nothing to do here
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("Error in stream reader", throwable);
    }
}

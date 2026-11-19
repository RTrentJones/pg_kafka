// Transactional producer scenarios the Kafka CLI tools cannot express. Used by
// interop/java_checks.py; compiled at run time against $KAFKA_HOME/libs/*.
//
//   java TxnScenario <bootstrap> <topic> <transactional.id> <committed> <aborted> [open]
//
// Commits <committed> records in one transaction, then writes <aborted> records in a second
// transaction and aborts it (an aborted transaction at the tail of the partition). With the
// optional "open" argument it then writes one more record in a third transaction and exits
// WITHOUT ending it (halt), leaving an open transaction that must keep the LSO in place.
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TxnScenario {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0], topic = args[1], txnId = args[2];
        int committed = Integer.parseInt(args[3]), aborted = Integer.parseInt(args[4]);
        boolean leaveOpen = args.length > 5 && args[5].equals("open");
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("transactional.id", txnId);
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.initTransactions();
        producer.beginTransaction();
        for (int i = 0; i < committed; i++) producer.send(new ProducerRecord<>(topic, 0, null, "c" + i));
        producer.commitTransaction();
        if (aborted > 0) {
            producer.beginTransaction();
            for (int i = 0; i < aborted; i++) producer.send(new ProducerRecord<>(topic, 0, null, "a" + i));
            producer.flush();
            producer.abortTransaction();
        }
        if (leaveOpen) {
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, null, "open")).get();
            System.out.println("open transaction left behind");
            System.out.flush();
            Runtime.getRuntime().halt(0);
        }
        producer.close();
        System.out.println("done");
    }
}

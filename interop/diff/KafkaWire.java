// A JSON-over-stdio Kafka wire client built on Apache Kafka's own generated protocol classes,
// so the differential harness (interop/diff/run.py) encodes requests and decodes responses
// exactly as the Java client does, for any API and version.
//
//   java -cp "$KAFKA_HOME/libs/*:classes" KafkaWire host:port
//
// One JSON object per stdin line, one JSON line back per request:
//   {"conn": "a", "api": 3, "version": 9, "body": {...}}
//       body is a partial request in the field names of Kafka's JSON schemas; every field
//       left out takes its default. Produce records are given as
//       {"records": [{"key": "k", "value": "v"}...], "producerId": -1, "producerEpoch": -1,
//        "baseSequence": -1, "transactional": false}.
//       Reply: {"body": {...}} (Fetch records decoded into batches) or {"error": "..."}
//       ("closed" when the broker closed the connection).
//   {"conn": "a", "op": "send", ...same...}   send without reading the response
//   {"conn": "a", "op": "recv", "api": 3, "version": 9}   read one response
//   {"conn": "a", "op": "close"}
//   {"op": "template", "api": 3, "version": 9}   the default request, to see the field names
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;

public class KafkaWire {
    static final ObjectMapper M = new ObjectMapper();
    static final JsonNodeFactory F = JsonNodeFactory.instance;
    static String host;
    static int port;
    static final Map<String, Socket> conns = new HashMap<>();
    static int correlation = 0;

    public static void main(String[] args) throws Exception {
        String[] hp = args[0].split(":");
        host = hp[0];
        port = Integer.parseInt(hp[1]);
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            if (line.isBlank()) continue;
            ObjectNode out = F.objectNode();
            try {
                JsonNode cmd = M.readTree(line);
                handle(cmd, out);
            } catch (EOFException | java.net.SocketException e) {
                out.put("error", "closed");
            } catch (java.net.SocketTimeoutException e) {
                out.put("error", "timeout");
            } catch (Exception e) {
                out.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            System.out.println(M.writeValueAsString(out));
            System.out.flush();
        }
    }

    static Socket conn(String name) throws Exception {
        Socket s = conns.get(name);
        if (s == null || s.isClosed()) {
            s = new Socket(host, port);
            s.setSoTimeout(Integer.getInteger("wire.timeout.ms", 30000));
            s.setTcpNoDelay(true);
            conns.put(name, s);
        }
        return s;
    }

    static void handle(JsonNode cmd, ObjectNode out) throws Exception {
        String c = cmd.path("conn").asText("default");
        String op = cmd.path("op").asText("request");
        if (op.equals("close")) {
            Socket s = conns.remove(c);
            if (s != null) s.close();
            return;
        }
        short key = (short) cmd.get("api").asInt();
        short version = (short) cmd.get("version").asInt();
        if (op.equals("template")) {
            out.set("request", write(ApiMessageType.fromApiKey(key).newRequest(), version));
            return;
        }
        if (!op.equals("recv")) {
            send(conn(c), key, version, cmd.path("body"));
        }
        if (!op.equals("send")) {
            out.set("body", recv(conn(c), key, version));
        }
    }

    static void send(Socket s, short key, short version, JsonNode partial) throws Exception {
        ApiKeys api = ApiKeys.forId(key);
        ApiMessageType type = ApiMessageType.fromApiKey(key);
        ApiMessage msg = type.newRequest();
        List<JsonNode> produceRecords = new ArrayList<>();
        JsonNode body = partial.deepCopy();
        if (key == ApiKeys.PRODUCE.id) {
            for (JsonNode t : body.path("topicData"))
                for (JsonNode p : t.path("partitionData")) {
                    produceRecords.add(p.get("records"));
                    ((ObjectNode) p).remove("records");
                }
        }
        JsonNode full = complete(body.isMissingNode() ? F.objectNode() : body, msg, version);
        msg = (ApiMessage) converter(msg.getClass()).getMethod("read", JsonNode.class, short.class)
                .invoke(null, full, version);
        if (key == ApiKeys.PRODUCE.id) {
            int i = 0;
            for (ProduceRequestData.TopicProduceData t : ((ProduceRequestData) msg).topicData())
                for (ProduceRequestData.PartitionProduceData p : t.partitionData())
                    p.setRecords(records(produceRecords.get(i++)));
        }
        RequestHeaderData header = new RequestHeaderData().setRequestApiKey(key)
                .setRequestApiVersion(version).setCorrelationId(++correlation)
                .setClientId("pgk-diff");
        short hv = api.requestHeaderVersion(version);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = header.size(cache, hv) + msg.size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(4 + size);
        buf.putInt(size);
        ByteBufferAccessor acc = new ByteBufferAccessor(buf);
        header.write(acc, cache, hv);
        msg.write(acc, cache, version);
        OutputStream os = s.getOutputStream();
        os.write(buf.array());
        os.flush();
    }

    static JsonNode recv(Socket s, short key, short version) throws Exception {
        DataInputStream in = new DataInputStream(s.getInputStream());
        int n = in.readInt();
        byte[] frame = new byte[n];
        in.readFully(frame);
        ByteBuffer buf = ByteBuffer.wrap(frame);
        ApiKeys api = ApiKeys.forId(key);
        ByteBufferAccessor acc = new ByteBufferAccessor(buf);
        new ResponseHeaderData(acc, api.responseHeaderVersion(version));
        ApiMessage resp = ApiMessageType.fromApiKey(key).newResponse();
        resp.read(acc, version);
        JsonNode json = write(resp, version);
        if (key == ApiKeys.FETCH.id) decodeFetchRecords((FetchResponseData) resp, (ObjectNode) json);
        return json;
    }

    // ------------------------------------------------------------------ records

    static MemoryRecords records(JsonNode spec) {
        if (spec == null || spec.isNull()) return MemoryRecords.EMPTY;
        List<SimpleRecord> recs = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (JsonNode r : spec.path("records")) {
            byte[] k = r.hasNonNull("key") ? r.get("key").asText().getBytes(StandardCharsets.UTF_8) : null;
            byte[] v = r.hasNonNull("value") ? r.get("value").asText().getBytes(StandardCharsets.UTF_8) : null;
            recs.add(new SimpleRecord(r.path("timestamp").asLong(now), k, v));
        }
        SimpleRecord[] arr = recs.toArray(new SimpleRecord[0]);
        long pid = spec.path("producerId").asLong(-1);
        short epoch = (short) spec.path("producerEpoch").asInt(-1);
        int seq = spec.path("baseSequence").asInt(-1);
        if (spec.path("transactional").asBoolean(false))
            return MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid, epoch, seq, arr);
        if (pid >= 0)
            return MemoryRecords.withIdempotentRecords(CompressionType.NONE, pid, epoch, seq, arr);
        return MemoryRecords.withRecords(CompressionType.NONE, arr);
    }

    static void decodeFetchRecords(FetchResponseData data, ObjectNode json) {
        Iterator<JsonNode> jt = json.path("responses").iterator();
        for (FetchResponseData.FetchableTopicResponse t : data.responses()) {
            Iterator<JsonNode> jp = jt.next().path("partitions").iterator();
            for (FetchResponseData.PartitionData p : t.partitions()) {
                ObjectNode pj = (ObjectNode) jp.next();
                ArrayNode batches = F.arrayNode();
                if (p.records() instanceof MemoryRecords) {
                    for (RecordBatch b : ((MemoryRecords) p.records()).batches()) {
                        ObjectNode bj = batches.addObject();
                        bj.put("baseOffset", b.baseOffset());
                        bj.put("lastOffset", b.lastOffset());
                        bj.put("producerId", b.producerId());
                        bj.put("producerEpoch", b.producerEpoch());
                        bj.put("baseSequence", b.baseSequence());
                        bj.put("isTransactional", b.isTransactional());
                        bj.put("isControlBatch", b.isControlBatch());
                        ArrayNode rs = bj.putArray("records");
                        for (Record r : b) {
                            ObjectNode rj = rs.addObject();
                            rj.put("offset", r.offset());
                            rj.put("key", r.hasKey() ? utf8(r.key()) : null);
                            rj.put("value", r.hasValue() ? utf8(r.value()) : null);
                        }
                    }
                }
                pj.set("records", batches);
            }
        }
    }

    static String utf8(ByteBuffer b) {
        byte[] a = new byte[b.remaining()];
        b.get(a);
        return new String(a, StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------ JSON <-> messages

    static Class<?> converter(Class<?> dataClass) throws ClassNotFoundException {
        // org...MetadataRequestData            -> org...MetadataRequestDataJsonConverter
        // org...MetadataRequestData$Topic      -> org...MetadataRequestDataJsonConverter$TopicJsonConverter
        String n = dataClass.getName();
        int dollar = n.indexOf('$');
        if (dollar < 0) return Class.forName(n + "JsonConverter");
        return Class.forName(n.substring(0, dollar) + "JsonConverter" + n.substring(dollar) + "JsonConverter");
    }

    static JsonNode write(Object data, short version) throws Exception {
        Class<?> conv = converter(data.getClass());
        for (Method m : conv.getMethods()) {
            if (m.getName().equals("write") && m.getParameterCount() == 3
                    && m.getParameterTypes()[2] == boolean.class)
                return (JsonNode) m.invoke(null, data, version, false);
        }
        return (JsonNode) conv.getMethod("write", data.getClass(), short.class).invoke(null, data, version);
    }

    /** The partial JSON object merged over the JSON of `defaults` (all fields valid at `version`). */
    static JsonNode complete(JsonNode partial, Object defaults, short version) throws Exception {
        ObjectNode full = (ObjectNode) write(defaults, version);
        Iterator<Map.Entry<String, JsonNode>> it = partial.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String f = e.getKey();
            JsonNode v = e.getValue();
            if (!full.has(f))
                throw new IllegalArgumentException(defaults.getClass().getSimpleName() + "." + f
                        + " does not exist at version " + version);
            Method getter = defaults.getClass().getMethod(f);
            if (v.isArray() && v.size() > 0 && v.get(0).isObject()) {
                Class<?> elem = elementClass(getter.getGenericReturnType());
                ArrayNode arr = F.arrayNode();
                for (JsonNode el : v)
                    arr.add(complete(el, elem.getDeclaredConstructor().newInstance(), version));
                full.set(f, arr);
            } else if (v.isObject() && !getter.getReturnType().isPrimitive()
                    && getter.getReturnType().getName().startsWith("org.apache.kafka.common.message")) {
                full.set(f, complete(v, getter.getReturnType().getDeclaredConstructor().newInstance(), version));
            } else {
                full.set(f, v);
            }
        }
        return full;
    }

    static Class<?> elementClass(Type t) {
        if (t instanceof ParameterizedType) {
            Type a = ((ParameterizedType) t).getActualTypeArguments()[0];
            return (Class<?>) a;
        }
        // ImplicitLinkedHashMultiCollection subclasses (e.g. MetadataRequestTopicCollection):
        // the element type is the collection's type parameter.
        Class<?> c = (Class<?>) t;
        Type sup = c.getGenericSuperclass();
        return (Class<?>) ((ParameterizedType) sup).getActualTypeArguments()[0];
    }
}

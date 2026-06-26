// Conformance harness — Sarama (Go), unmodified, against pg_kafka on PG_KAFKA_BROKER (:9092).
//
// Each of the 23 implemented APIs is probed independently via mark(); a failing probe records
// "fail" for that one cell and the run continues. Some wire APIs are inferred from the higher-level
// op that drives them (a working group consume implies FindCoordinator/SyncGroup/Heartbeat).
// Writes results-sarama.json.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type apiEntry struct {
	Key  int    `json:"key"`
	Name string `json:"name"`
}

type clientResult struct {
	Name    string            `json:"name"`
	Lang    string            `json:"lang"`
	Version string            `json:"version"`
	Results map[string]string `json:"results"`
}

var results = map[string]string{}

// mark runs a probe and records pass | fail; it is panic-safe so a surprise (e.g. a nil admin after
// a failed constructor) becomes a "fail" cell rather than killing the whole run.
func mark(name string, fn func() error) {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		err = fn()
	}()
	if err != nil {
		results[name] = "fail"
		fmt.Fprintf(os.Stderr, "[sarama] %s: %v\n", name, err)
		return
	}
	if results[name] != "fail" {
		results[name] = "pass"
	}
}

// groupHandler signals on the first message received (across any partition claim).
type groupHandler struct {
	got  chan struct{}
	once sync.Once
}

func (h *groupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *groupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		sess.MarkMessage(msg, "")
		sess.Commit()
		h.once.Do(func() { close(h.got) })
		return nil
	}
	return nil
}

func main() {
	broker := os.Getenv("PG_KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}
	brokers := []string{broker}

	apisPath := os.Getenv("APIS_JSON")
	if apisPath == "" {
		apisPath = filepath.Join("..", "apis.json")
	}
	raw, err := os.ReadFile(apisPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot read %s: %v\n", apisPath, err)
		os.Exit(1)
	}
	var doc struct {
		Apis []apiEntry `json:"apis"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		fmt.Fprintf(os.Stderr, "bad apis.json: %v\n", err)
		os.Exit(1)
	}
	for _, a := range doc.Apis {
		results[a.Name] = "na"
	}

	stamp := time.Now().Unix()
	topic := fmt.Sprintf("conf-go-%d", stamp)
	group := fmt.Sprintf("conf-go-grp-%d", stamp)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Producer.Return.Successes = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Client creation negotiates ApiVersions and fetches Metadata.
	var client sarama.Client
	mark("ApiVersions", func() error {
		c, err := sarama.NewClient(brokers, cfg)
		if err != nil {
			return err
		}
		client = c
		return nil
	})
	if client == nil {
		writeOut() // nothing reachable — emit the all-na/fail row and stop
		return
	}
	defer client.Close()
	mark("Metadata", func() error { _, err := client.Topics(); return err })

	admin, adminErr := sarama.NewClusterAdminFromClient(client)
	mark("CreateTopics", func() error {
		if adminErr != nil {
			return adminErr
		}
		return admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}, false)
	})
	time.Sleep(time.Second)
	mark("CreatePartitions", func() error { return admin.CreatePartitions(topic, 3, nil, false) })

	mark("Produce", func() error {
		producer, err := sarama.NewSyncProducerFromClient(client)
		if err != nil {
			return err
		}
		defer producer.Close()
		for _, kv := range [][2]string{{"k1", "v1"}, {"k2", "v2"}} {
			if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic, Key: sarama.StringEncoder(kv[0]), Value: sarama.StringEncoder(kv[1]),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	mark("ListOffsets", func() error { _, err := client.GetOffset(topic, 0, sarama.OffsetNewest); return err })

	// Group consume → Join/Sync/Heartbeat/Fetch.
	mark("Fetch", func() error {
		cg, err := sarama.NewConsumerGroupFromClient(group, client)
		if err != nil {
			return err
		}
		defer cg.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		h := &groupHandler{got: make(chan struct{})}
		go func() { _ = cg.Consume(ctx, []string{topic}, h) }()
		select {
		case <-h.got:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("no messages consumed within timeout")
		}
	})
	if results["Fetch"] == "pass" {
		for _, k := range []string{"FindCoordinator", "JoinGroup", "SyncGroup", "Heartbeat", "OffsetCommit", "LeaveGroup"} {
			results[k] = "pass"
		}
	}

	mark("OffsetFetch", func() error {
		_, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{topic: {0}})
		return err
	})
	mark("ListGroups", func() error { _, err := admin.ListConsumerGroups(); return err })
	mark("DescribeGroups", func() error { _, err := admin.DescribeConsumerGroups([]string{group}); return err })
	mark("DeleteGroups", func() error { return admin.DeleteConsumerGroup(group) })

	// Transactions (EOS): a transactional producer performs InitProducerId on creation; a committed
	// txn drives AddPartitionsToTxn + EndTxn; AddOffsetsToTxn drives AddOffsetsToTxn + TxnOffsetCommit.
	txnCfg := sarama.NewConfig()
	txnCfg.Version = sarama.V2_8_0_0
	txnCfg.Producer.Idempotent = true
	txnCfg.Producer.Return.Successes = true
	txnCfg.Producer.RequiredAcks = sarama.WaitForAll
	txnCfg.Net.MaxOpenRequests = 1
	txnCfg.Producer.Transaction.ID = fmt.Sprintf("conf-go-tx-%d", stamp)

	var txnProducer sarama.AsyncProducer
	mark("InitProducerId", func() error {
		p, err := sarama.NewAsyncProducer(brokers, txnCfg)
		if err != nil {
			return err
		}
		txnProducer = p
		return nil
	})
	if txnProducer != nil {
		defer txnProducer.Close()
		mark("AddPartitionsToTxn", func() error {
			if err := txnProducer.BeginTxn(); err != nil {
				return err
			}
			txnProducer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder("go-tx-1")}
			select {
			case <-txnProducer.Successes():
			case err := <-txnProducer.Errors():
				return err.Err
			case <-time.After(10 * time.Second):
				return fmt.Errorf("txn produce timed out")
			}
			return txnProducer.CommitTxn()
		})
		if results["AddPartitionsToTxn"] == "pass" {
			results["EndTxn"] = "pass"
		}
		mark("AddOffsetsToTxn", func() error {
			if err := txnProducer.BeginTxn(); err != nil {
				return err
			}
			offsets := map[string][]*sarama.PartitionOffsetMetadata{
				topic: {{Partition: 0, Offset: 2}},
			}
			if err := txnProducer.AddOffsetsToTxn(offsets, group); err != nil {
				_ = txnProducer.AbortTxn()
				return err
			}
			return txnProducer.CommitTxn()
		})
		if results["AddOffsetsToTxn"] == "pass" {
			results["TxnOffsetCommit"] = "pass"
		}
	}

	mark("DeleteTopics", func() error { return admin.DeleteTopic(topic) })
	writeOut()
}

func writeOut() {
	out := os.Getenv("OUT")
	if out == "" {
		out = "results-sarama.json"
	}
	res := clientResult{Name: "Sarama", Lang: "Go", Version: "v1.43.x", Results: results}
	b, _ := json.MarshalIndent(res, "", "  ")
	if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", out, err)
		os.Exit(1)
	}
	tally := map[string]int{}
	for _, s := range results {
		tally[s]++
	}
	fmt.Printf("[sarama] wrote %s — %v\n", out, tally)
}

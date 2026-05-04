/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.Document;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforms for the MongoDB to MongoDB template. */
public class MongoDbTransforms {

  public static WriteWithDlq writeWithDlq() {
    return new WriteWithDlq();
  }

  public static class WriteWithDlq extends PTransform<PCollection<Document>, PDone> {
    private String uri;
    private String database;
    private String collection;
    private Integer batchSize = 5000;
    private Boolean ordered = false;
    private String writeConcern;
    private Boolean journal;
    private String dlqPath;
    private Integer maxConcurrentAsyncWrites = 10;

    public WriteWithDlq withUri(String uri) {
      this.uri = uri;
      return this;
    }

    public WriteWithDlq withDatabase(String database) {
      this.database = database;
      return this;
    }

    public WriteWithDlq withCollection(String collection) {
      this.collection = collection;
      return this;
    }

    public WriteWithDlq withBatchSize(Integer batchSize) {
      if (batchSize != null) {
        this.batchSize = batchSize;
      }
      return this;
    }

    public WriteWithDlq withOrdered(Boolean ordered) {
      if (ordered != null) {
        this.ordered = ordered;
      }
      return this;
    }

    public WriteWithDlq withWriteConcern(String writeConcern) {
      this.writeConcern = writeConcern;
      return this;
    }

    public WriteWithDlq withJournal(Boolean journal) {
      this.journal = journal;
      return this;
    }

    public WriteWithDlq withDlqPath(String dlqPath) {
      this.dlqPath = dlqPath;
      return this;
    }

    public WriteWithDlq withMaxConcurrentAsyncWrites(Integer maxConcurrentAsyncWrites) {
      if (maxConcurrentAsyncWrites != null) {
        this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
      }
      return this;
    }

    @Override
    public PDone expand(PCollection<Document> input) {
      TupleTag<Document> successTag = new TupleTag<Document>() {};
      TupleTag<String> failureTag = new TupleTag<String>() {};
      TupleTag<String> severeFailureTag = new TupleTag<String>() {};

      PCollectionTuple writeResults =
          input
              .apply(
                  "AddRandomKey",
                  WithKeys.of(
                      doc ->
                          String.valueOf(
                              java.util.concurrent.ThreadLocalRandom.current().nextInt(1000))))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Document.class)))
              .apply("GroupIntoBatches", GroupIntoBatches.ofSize(batchSize))
              .apply(
                  "WriteBatches",
                  ParDo.of(
                      new WriteBatchesFn(
                          uri,
                          database,
                          collection,
                          ordered,
                          writeConcern,
                          journal,
                          maxConcurrentAsyncWrites,
                          failureTag,
                          severeFailureTag))
                      .withOutputTags(successTag, TupleTagList.of(failureTag).and(severeFailureTag)));

      if (dlqPath != null && !dlqPath.isEmpty()) {
        writeResults
            .get(failureTag)
            .apply("WriteDlq_" + collection, TextIO.write().to(dlqPath + "/" + collection));
        
        writeResults
            .get(severeFailureTag)
            .apply("WriteSevereDlq_" + collection, TextIO.write().to(dlqPath + "/severe_failures/" + collection));
      }

      return PDone.in(input.getPipeline());
    }
  }
  public static class WriteBatchesFn extends DoFn<KV<String, Iterable<Document>>, Document> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteBatchesFn.class);
    private static final int INVALID_ARGUMENT = 2;
    private final String uri;
    private final String database;
    private final String collection;
    private final Boolean ordered;
    private final String writeConcern;
    private final Boolean journal;
    private final Integer maxConcurrentAsyncWrites;
    private final TupleTag<String> failureTag;
    private final TupleTag<String> severeFailureTag;
    private transient MongoClient client;

    public WriteBatchesFn(
        String uri,
        String database,
        String collection,
        Boolean ordered,
        String writeConcern,
        Boolean journal,
        Integer maxConcurrentAsyncWrites,
        TupleTag<String> failureTag,
        TupleTag<String> severeFailureTag) {
      this.uri = uri;
      this.database = database;
      this.collection = collection;
      this.ordered = ordered;
      this.writeConcern = writeConcern;
      this.journal = journal;
      this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
      this.failureTag = failureTag;
      this.severeFailureTag = severeFailureTag;
    }

    @com.google.common.annotations.VisibleForTesting
    public WriteBatchesFn(
        MongoClient client,
        String database,
        String collection,
        Boolean ordered,
        String writeConcern,
        Boolean journal,
        Integer maxConcurrentAsyncWrites,
        TupleTag<String> failureTag,
        TupleTag<String> severeFailureTag) {
      this.client = client;
      this.uri = "";
      this.database = database;
      this.collection = collection;
      this.ordered = ordered;
      this.writeConcern = writeConcern;
      this.journal = journal;
      this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
      this.failureTag = failureTag;
      this.severeFailureTag = severeFailureTag;
    }

    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> mongoCollection;
    private transient ExecutorService executor;
    private transient Semaphore semaphore;
    private transient ConcurrentLinkedQueue<CompletableFuture<Void>> futures;
    private transient ConcurrentLinkedQueue<String> failures;

    @Setup
    public void setup() {
      executor = Executors.newFixedThreadPool(maxConcurrentAsyncWrites);
      semaphore = new Semaphore(maxConcurrentAsyncWrites);
    }

    @Teardown
    public void teardown() {
      if (executor != null) {
        executor.shutdown();
      }
    }

    @StartBundle
    public void startBundle() {
      if (client != null) {
        mongoClient = client;
      } else {
        mongoClient = MongoClients.create(uri);
      }
      MongoDatabase db = mongoClient.getDatabase(database);

      WriteConcern wc = WriteConcern.ACKNOWLEDGED;
      if (writeConcern != null) {
        if (writeConcern.equalsIgnoreCase("majority")) {
          wc = WriteConcern.MAJORITY;
        } else {
          try {
            int w = Integer.parseInt(writeConcern);
            wc = new WriteConcern(w);
          } catch (NumberFormatException e) {
            // Fallback to default
          }
        }
      }
      if (journal != null) {
        wc = wc.withJournal(journal);
      }

      mongoCollection = db.getCollection(collection).withWriteConcern(wc);

      futures = new ConcurrentLinkedQueue<>();
      failures = new ConcurrentLinkedQueue<>();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException {
      Iterable<Document> docs = c.element().getValue();
      List<WriteModel<Document>> updates = new ArrayList<>();
      List<Document> docList = new ArrayList<>();

      for (Document doc : docs) {
        docList.add(doc);
        Object id = doc.get("_id");
        if (id != null) {
          updates.add(
              new ReplaceOneModel<>(
                  new Document("_id", id),
                  doc,
                  new ReplaceOptions().upsert(true)));
        }
      }

      if (!updates.isEmpty()) {
        semaphore.acquire();
        CompletableFuture<Void> future =
            CompletableFuture.runAsync(
                () -> {
                  try {
                    int retryCount = 0;
                    int maxRetries = 3;
                    long retryDelayMs = 1000;
                    boolean success = false;

                    while (retryCount <= maxRetries && !success) {
                    try {
                      mongoCollection.bulkWrite(updates, new BulkWriteOptions().ordered(ordered));
                      success = true;
                    } catch (MongoBulkWriteException e) {
                      // Permanent data errors, do not retry, isolate and DLQ
                      Set<Integer> failedIndices = new HashSet<>();
                      for (BulkWriteError error : e.getWriteErrors()) {
                        failedIndices.add(error.getIndex());
                        Document failedDoc = docList.get(error.getIndex());
                        String errorMessage = failedDoc.toJson() + " - Error: " + error.getMessage();
                        if (error.getCode() == INVALID_ARGUMENT) {
                          failures.add("SEVERE:" + errorMessage);
                        } else {
                          failures.add(errorMessage);
                        }
                      }

                      if (ordered) {
                        int firstErrorIndex = docList.size();
                        for (BulkWriteError error : e.getWriteErrors()) {
                          firstErrorIndex = Math.min(firstErrorIndex, error.getIndex());
                        }
                        for (int i = firstErrorIndex + 1; i < docList.size(); i++) {
                          Document doc = docList.get(i);
                          failures.add(doc.toJson() + " - Error: Bulk write stopped due to previous error");
                        }
                      }
                      success = true; // Consider handled, don't retry batch
                    } catch (Exception e) {
                      // Potential transient error (network, timeout, etc.)
                      if (retryCount < maxRetries) {
                        LOG.warn("Transient error during bulk write, retrying...", e);
                        try {
                          Thread.sleep(retryDelayMs * (retryCount + 1));
                        } catch (InterruptedException ie) {
                          Thread.currentThread().interrupt();
                          break;
                        }
                        retryCount++;
                      } else {
                        // Max retries reached, fail whole batch
                        LOG.error("Bulk write failed after max retries", e);
                        for (Document doc : docList) {
                          failures.add(doc.toJson() + " - Error: " + e.getMessage());
                        }
                        break;
                      }
                    }
                  }
                } finally {
                    semaphore.release();
                  }
                },
                executor);
        futures.add(future);
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      if (client == null && mongoClient != null) {
        mongoClient.close();
      }

      String failure;
      while ((failure = failures.poll()) != null) {
        if (failure.startsWith("SEVERE:")) {
          c.output(severeFailureTag, failure.substring(7), Instant.now(), GlobalWindow.INSTANCE);
        } else {
          c.output(failureTag, failure, Instant.now(), GlobalWindow.INSTANCE);
        }
      }
    }
  }
}

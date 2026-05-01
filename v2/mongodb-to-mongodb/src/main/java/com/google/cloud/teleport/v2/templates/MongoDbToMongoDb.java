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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.transforms.MongoDbTransforms;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.mongodb.FindQuery;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dataflow template which copies data from one MongoDB database to another. */
@Template(
    name = "Mongodb_To_Mongodb",
    category = TemplateCategory.BATCH,
    displayName = "MongoDB to MongoDB",
    description = "Copy data from one MongoDB database to another.",
    flexContainerName = "mongodb-to-mongodb",
    optionsClass = MongoDbToMongoDb.Options.class)
public class MongoDbToMongoDb {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDbToMongoDb.class);

  public interface Options extends JavascriptTextTransformerOptions {
    @TemplateParameter.Text(
        order = 1,
        groupName = "Source",
        description = "Source MongoDB Connection URI",
        helpText = "URI to connect to the source MongoDB cluster.")
    @Validation.Required
    String getSourceUri();

    void setSourceUri(String value);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Target",
        description = "Target MongoDB Connection URI",
        helpText = "URI to connect to the target MongoDB cluster.")
    @Validation.Required
    String getTargetUri();

    void setTargetUri(String value);

    @TemplateParameter.Text(
        order = 3,
        groupName = "Source",
        description = "Source MongoDB Database",
        helpText = "Database in the source MongoDB to read from.")
    @Validation.Required
    String getSourceDatabase();

    void setSourceDatabase(String value);

    @TemplateParameter.Text(
        order = 4,
        groupName = "Target",
        description = "Target MongoDB Database",
        helpText = "Database in the target MongoDB to write to.")
    @Validation.Required
    String getTargetDatabase();

    void setTargetDatabase(String value);

    @TemplateParameter.Text(
        order = 5,
        groupName = "Source",
        optional = true,
        description = "Source MongoDB Collection",
        helpText =
            "Collection in the source MongoDB to read from. If not provided, all collections in the"
                + " database will be migrated. Note: If not provided, the machine submitting the job must have network access to the source MongoDB instance to list collections.")
    String getSourceCollection();

    void setSourceCollection(String value);

    @TemplateParameter.Text(
        order = 6,
        groupName = "Target",
        optional = true,
        description = "Target MongoDB Collection",
        helpText =
            "Collection in the target MongoDB to write to. If not provided, source collection names"
                + " will be used.")
    String getTargetCollection();

    void setTargetCollection(String value);

    @TemplateParameter.Text(
        order = 7,
        groupName = "Source",
        optional = true,
        description = "BSON Filter",
        helpText = "BSON query for server-side filtering.")
    String getFilter();

    void setFilter(String value);

    @TemplateParameter.Text(
        order = 8,
        groupName = "Source",
        optional = true,
        description = "Projection",
        helpText = "Fields to include or exclude.")
    String getProjection();

    void setProjection(String value);

    @TemplateParameter.Boolean(
        order = 9,
        groupName = "Source",
        optional = true,
        description = "Use BucketAuto",
        helpText = "Enable withBucketAuto for Atlas compatibility.")
    @Default.Boolean(false)
    Boolean getUseBucketAuto();

    void setUseBucketAuto(Boolean value);

    @TemplateParameter.Integer(
        order = 10,
        groupName = "Source",
        optional = true,
        description = "Number of Splits",
        helpText = "Suggest a specific number of partitions for reading.")
    Integer getNumSplits();

    void setNumSplits(Integer value);

    @TemplateParameter.Integer(
        order = 11,
        groupName = "Target",
        optional = true,
        description = "Batch Size",
        helpText = "Number of documents in a bulk write.")
    @Default.Integer(5000)
    Integer getBatchSize();

    void setBatchSize(Integer value);

    @TemplateParameter.Boolean(
        order = 12,
        groupName = "Target",
        optional = true,
        description = "Ordered Bulk Write",
        helpText = "Allow parallel execution and prevent batch failure on single error.")
    @Default.Boolean(false)
    Boolean getOrdered();

    void setOrdered(Boolean value);

    @TemplateParameter.Text(
        order = 13,
        groupName = "Target",
        optional = true,
        description = "Write Concern",
        helpText = "Acknowledgment level (e.g., 'w: 1').")
    String getWriteConcern();

    void setWriteConcern(String value);

    @TemplateParameter.Boolean(
        order = 14,
        groupName = "Target",
        optional = true,
        description = "Journaling",
        helpText = "Enable/disable journaling.")
    Boolean getJournal();

    void setJournal(Boolean value);

    @TemplateParameter.Text(
        order = 15,
        optional = true,
        description = "Process DLQ Path",
        helpText = "Path to store failed events during processing.")
    String getProcessDlqPath();

    void setProcessDlqPath(String value);

    @TemplateParameter.Text(
        order = 16,
        optional = true,
        description = "Write DLQ Path",
        helpText = "Path to store failed events during writing.")
    String getWriteDlqPath();

    void setWriteDlqPath(String value);

    @TemplateParameter.Integer(
        order = 17,
        optional = true,
        description = "Max Concurrent Async Writes",
        helpText = "Maximum number of concurrent asynchronous batch writes per worker.")
    @Default.Integer(10)
    Integer getMaxConcurrentAsyncWrites();

    void setMaxConcurrentAsyncWrites(Integer value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  public static void run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    String sourceUri = options.getSourceUri();
    String sourceDatabase = options.getSourceDatabase();
    String sourceCollection = options.getSourceCollection();

    List<String> collections = new ArrayList<>();
    if (sourceCollection != null && !sourceCollection.isEmpty()) {
      collections.add(sourceCollection);
    } else {
      LOG.warn("Source collection not specified. Attempting to list all collections from source database. This requires network access from the submission machine.");
      // List collections from source
      try (MongoClient mongoClient = MongoClients.create(sourceUri)) {
        MongoDatabase db = mongoClient.getDatabase(sourceDatabase);
        for (String name : db.listCollectionNames()) {
          collections.add(name);
        }
      }
    }

    for (String collection : collections) {
      String targetCollection = options.getTargetCollection();
      if (targetCollection == null || targetCollection.isEmpty()) {
        targetCollection = collection; // Use source collection name if target not provided
      }

      MongoDbIO.Read read =
          MongoDbIO.read()
              .withUri(options.getSourceUri())
              .withDatabase(options.getSourceDatabase())
              .withCollection(collection);

      String filterJson = options.getFilter();
      if (filterJson != null && !filterJson.isEmpty()) {
        BsonDocument filter = BsonDocument.parse(filterJson);
        read = read.withQueryFn(FindQuery.create().withFilters(filter));
      }

      String projectionStr = options.getProjection();
      if (projectionStr != null && !projectionStr.isEmpty()) {
        List<String> projection = Arrays.asList(projectionStr.split(","));
        read = read.withQueryFn(FindQuery.create().withProjection(projection));
      }

      if (options.getUseBucketAuto() != null && options.getUseBucketAuto()) {
        read = read.withBucketAuto(true);
      }

      if (options.getNumSplits() != null) {
        read = read.withNumSplits(options.getNumSplits());
      }

      PCollection<Document> documents = pipeline.apply("Read_" + collection, read);

      // Check if UDF path is provided to enable transformation
      if (!Strings.isNullOrEmpty(options.getJavascriptTextTransformGcsPath())) {
        TupleTag<FailsafeElement<Document, String>> udfSuccessTag = new TupleTag<>();
        TupleTag<FailsafeElement<Document, String>> udfFailureTag = new TupleTag<>();
        TupleTag<FailsafeElement<Document, String>> convertSuccessTag = new TupleTag<>();
        TupleTag<FailsafeElement<Document, String>> convertFailureTag = new TupleTag<>();

        // The FailsafeJavascriptUdf expects a PCollection of FailsafeElement where the payload is a
        // String.
        // We convert the Document to its JSON string representation to be processed by the UDF.
        // The original Document is preserved as the original payload to enable recovery or DLQ
        // routing.
        PCollectionTuple convertResult =
            documents.apply(
                "ConvertToFailsafe_" + collection,
                ParDo.of(new ConvertToFailsafeFn(convertFailureTag))
                    .withOutputTags(convertSuccessTag, TupleTagList.of(convertFailureTag)));

        PCollection<FailsafeElement<Document, String>> failsafeElements =
            convertResult
                .get(convertSuccessTag)
                // Explicitly set the coder for FailsafeElement because Beam cannot always infer it
                // for parameterized types.
                .setCoder(
                    FailsafeElementCoder.of(
                        SerializableCoder.of(Document.class), StringUtf8Coder.of()));

        // Handle convert failures by routing to DLQ if a path is configured.
        writeFailsafeElementDlq(
            convertResult.get(convertFailureTag),
            "Convert",
            "convert_failures",
            collection,
            options);

        // Apply the JavaScript UDF to the JSON payload.
        PCollectionTuple udfResult =
            failsafeElements.apply(
                "RunUDF_" + collection,
                FailsafeJavascriptUdf.<Document>newBuilder()
                    .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                    .setFunctionName(options.getJavascriptTextTransformFunctionName())
                    .setReloadIntervalMinutes(
                        options.getJavascriptTextTransformReloadIntervalMinutes())
                    .setSuccessTag(udfSuccessTag)
                    .setFailureTag(udfFailureTag)
                    .build());

        // Explicitly set coders for the output collections of the UDF transform.
        udfResult.get(udfSuccessTag)
            .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));
        udfResult.get(udfFailureTag)
            .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));

        // Handle failed UDF processing by routing to DLQ if a path is configured.
        writeFailsafeElementDlq(
            udfResult.get(udfFailureTag),
            "Udf",
            "udf_failures",
            collection,
            options);

        // Extract success and convert back to Document
        TupleTag<Document> parseSuccessTag = new TupleTag<>();
        TupleTag<FailsafeElement<Document, String>> parseFailureTag = new TupleTag<>();

        PCollectionTuple parseResult =
            udfResult
                .get(udfSuccessTag)
                .apply(
                    "ConvertFromFailsafe_" + collection,
                    ParDo.of(new ConvertFromFailsafeWithFailureFn(parseFailureTag))
                        .withOutputTags(parseSuccessTag, TupleTagList.of(parseFailureTag)));

        documents = parseResult.get(parseSuccessTag);

        // Handle parse failures by routing to DLQ if a path is configured.
        writeFailsafeElementDlq(
            parseResult.get(parseFailureTag),
            "Parse",
            "parse_failures",
            collection,
            options);
      }

      // Validation Stage with DLQ
      TupleTag<Document> successTag = new TupleTag<Document>() {};
      TupleTag<String> failureTag = new TupleTag<String>() {};

      PCollectionTuple processed =
          documents.apply(
              "Validate_" + collection,
              ParDo.of(new ValidateFn(successTag, failureTag))
                  .withOutputTags(successTag, TupleTagList.of(failureTag)));

      // Write Process Failures to DLQ
      writeStringDlq(
          processed.get(failureTag),
          "WriteProcessDlq_" + collection,
          options.getProcessDlqPath() + "/" + collection,
          options);

      // Write Stage with DLQ
      PCollection<Document> validDocs = processed.get(successTag);

      validDocs.apply(
          "Write_" + collection,
          MongoDbTransforms.writeWithDlq()
              .withUri(options.getTargetUri())
              .withDatabase(options.getTargetDatabase())
              .withCollection(targetCollection)
              .withBatchSize(options.getBatchSize())
              .withOrdered(options.getOrdered())
              .withWriteConcern(options.getWriteConcern())
              .withJournal(options.getJournal())
              .withDlqPath(options.getWriteDlqPath())
              .withMaxConcurrentAsyncWrites(options.getMaxConcurrentAsyncWrites()));
    }

    pipeline.run();
  }

  static class ConvertToFailsafeFn extends DoFn<Document, FailsafeElement<Document, String>> {
    private final TupleTag<FailsafeElement<Document, String>> convertFailureTag;

    ConvertToFailsafeFn(TupleTag<FailsafeElement<Document, String>> convertFailureTag) {
      this.convertFailureTag = convertFailureTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Document doc = c.element();
      try {
        c.output(FailsafeElement.of(doc, doc.toJson()));
      } catch (Exception e) {
        c.output(convertFailureTag, FailsafeElement.of(doc, "")
            .setErrorMessage(e.getMessage())
            .setStacktrace(java.util.Arrays.deepToString(e.getStackTrace())));
      }
    }
  }

  static class WriteDlqFn extends DoFn<FailsafeElement<Document, String>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      FailsafeElement<Document, String> element = c.element();
      c.output(element.toString());
    }
  }

  static class ConvertFromFailsafeWithFailureFn extends DoFn<FailsafeElement<Document, String>, Document> {
    private final TupleTag<FailsafeElement<Document, String>> parseFailureTag;

    ConvertFromFailsafeWithFailureFn(TupleTag<FailsafeElement<Document, String>> parseFailureTag) {
      this.parseFailureTag = parseFailureTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      FailsafeElement<Document, String> element = c.element();
      try {
        c.output(Document.parse(element.getPayload()));
      } catch (Exception e) {
        c.output(parseFailureTag, FailsafeElement.of(element.getOriginalPayload(), element.getPayload())
            .setErrorMessage(e.getMessage())
            .setStacktrace(java.util.Arrays.deepToString(e.getStackTrace())));
      }
    }
  }

  static class ValidateFn extends DoFn<Document, Document> {
    private final TupleTag<Document> successTag;
    private final TupleTag<String> failureTag;

    ValidateFn(TupleTag<Document> successTag, TupleTag<String> failureTag) {
      this.successTag = successTag;
      this.failureTag = failureTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Document doc = c.element();
      if (doc == null) {
        c.output(failureTag, "Null document");
      } else {
        c.output(successTag, doc);
      }
    }
  }

  private static void writeStringDlq(
      PCollection<String> failures,
      String transformName,
      String path,
      Options options) {
    if (options.getProcessDlqPath() != null && !options.getProcessDlqPath().isEmpty()) {
      failures.apply(
          transformName,
          TextIO.write().to(path));
    }
  }

  private static void writeFailsafeElementDlq(
      PCollection<FailsafeElement<Document, String>> failures,
      String transformPrefix,
      String subDir,
      String collection,
      Options options) {
    
    PCollection<String> stringFailures = failures
        .apply(
            transformPrefix + "WriteDlq_" + collection,
            ParDo.of(new WriteDlqFn()));
    
    writeStringDlq(
        stringFailures,
        transformPrefix + "WriteDlqText_" + collection,
        options.getProcessDlqPath() + "/" + subDir + "/" + collection,
        options);
  }
}

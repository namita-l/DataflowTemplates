package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.transforms.MongoDbTransforms;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MongoDbToMongoDbTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final TupleTag<FailsafeElement<Document, String>> SUCCESS_TAG =
      new TupleTag<FailsafeElement<Document, String>>() {};

  private static final TupleTag<FailsafeElement<Document, String>> FAILURE_TAG =
      new TupleTag<FailsafeElement<Document, String>>() {};

  @Test
  public void testUdfTransformation() throws IOException {
    // Create a temp JS file
    String jsCode =
        "function transform(inJson) {\n"
            + "  var obj = JSON.parse(inJson);\n"
            + "  obj.transformed = true;\n"
            + "  return JSON.stringify(obj);\n"
            + "}";
    Path tempFile = Files.createTempFile("transform", ".js");
    Files.write(tempFile, jsCode.getBytes());

    Document doc = new Document("name", "John").append("age", 30);
    List<Document> inputDocs = Arrays.asList(doc);

    PCollection<Document> documents = pipeline.apply("CreateInput", Create.of(inputDocs));

    // This mimics the logic in MongoDbToMongoDb.java
    PCollection<FailsafeElement<Document, String>> failsafeElements =
        documents.apply(
            "ConvertToFailsafe",
            ParDo.of(new ConvertToFailsafeFn()))
            .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));

    PCollectionTuple udfResult =
        failsafeElements.apply(
            "RunUDF",
            FailsafeJavascriptUdf.<Document>newBuilder()
                .setFileSystemPath(tempFile.toAbsolutePath().toString())
                .setFunctionName("transform")
                .setReloadIntervalMinutes(0)
                .setSuccessTag(SUCCESS_TAG)
                .setFailureTag(FAILURE_TAG)
                .build());

    udfResult.get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));
    udfResult.get(FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));

    PCollection<Document> transformedDocs =
        udfResult
            .get(SUCCESS_TAG)
            .apply(
                "ConvertFromFailsafe",
                ParDo.of(new ConvertFromFailsafeFn()));

    Document expectedDoc = new Document("name", "John").append("age", 30).append("transformed", true);

    PAssert.that(transformedDocs).containsInAnyOrder(expectedDoc);

    pipeline.run();

    // Clean up
    Files.delete(tempFile);
  }

  @Test
  public void testUdfParseFailureDlq() throws IOException {
    // Create a temp JS file that returns invalid JSON
    String jsCode =
        "function transform(inJson) {\n"
            + "  return \"invalid json\";\n"
            + "}";
    Path tempFile = Files.createTempFile("transform_invalid", ".js");
    Files.write(tempFile, jsCode.getBytes());

    Document doc = new Document("name", "John").append("age", 30);
    List<Document> inputDocs = Arrays.asList(doc);

    PCollection<Document> documents = pipeline.apply("CreateInput_Fail", Create.of(inputDocs));

    PCollection<FailsafeElement<Document, String>> failsafeElements =
        documents.apply(
            "ConvertToFailsafe_Fail",
            ParDo.of(new ConvertToFailsafeFn()))
            .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));

    PCollectionTuple udfResult =
        failsafeElements.apply(
            "RunUDF_Fail",
            FailsafeJavascriptUdf.<Document>newBuilder()
                .setFileSystemPath(tempFile.toAbsolutePath().toString())
                .setFunctionName("transform")
                .setReloadIntervalMinutes(0)
                .setSuccessTag(SUCCESS_TAG)
                .setFailureTag(FAILURE_TAG)
                .build());

    udfResult.get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));
    udfResult.get(FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));

    TupleTag<Document> parseSuccessTag = new TupleTag<>();
    TupleTag<FailsafeElement<Document, String>> parseFailureTag = new TupleTag<>();

    PCollectionTuple parseResult =
        udfResult
            .get(SUCCESS_TAG)
            .apply(
                "ConvertFromFailsafe_Fail",
                ParDo.of(
                    new ConvertFromFailsafeWithFailureFn(parseFailureTag))
                    .withOutputTags(parseSuccessTag, TupleTagList.of(parseFailureTag)));

    parseResult.get(parseFailureTag)
        .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));

    PAssert.that(parseResult.get(parseFailureTag))
        .satisfies(
            collection -> {
              FailsafeElement<Document, String> result = collection.iterator().next();
              assertEquals("invalid json", result.getPayload());
              return null;
            });

    pipeline.run();

    // Clean up
    Files.delete(tempFile);
  }

  @Test
  public void testUdfExecutionFailureDlq() throws IOException {
    // Create a temp JS file that throws an exception
    String jsCode =
        "function transform(inJson) {\n"
            + "  throw new Error(\"Simulated UDF failure\");\n"
            + "}";
    Path tempFile = Files.createTempFile("transform_fail", ".js");
    Files.write(tempFile, jsCode.getBytes());

    Document doc = new Document("name", "John").append("age", 30);
    List<Document> inputDocs = Arrays.asList(doc);

    PCollection<Document> documents = pipeline.apply("CreateInput_UdfFail", Create.of(inputDocs));

    PCollection<FailsafeElement<Document, String>> failsafeElements =
        documents.apply(
            "ConvertToFailsafe_UdfFail",
            ParDo.of(new ConvertToFailsafeFn()))
            .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));

    PCollectionTuple udfResult =
        failsafeElements.apply(
            "RunUDF_UdfFail",
            FailsafeJavascriptUdf.<Document>newBuilder()
                .setFileSystemPath(tempFile.toAbsolutePath().toString())
                .setFunctionName("transform")
                .setReloadIntervalMinutes(0)
                .setSuccessTag(SUCCESS_TAG)
                .setFailureTag(FAILURE_TAG)
                .build());

    udfResult.get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));
    udfResult.get(FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));

    PAssert.that(udfResult.get(FAILURE_TAG))
        .satisfies(
            collection -> {
              FailsafeElement<Document, String> result = collection.iterator().next();
              assertEquals("John", result.getOriginalPayload().getString("name"));
              assertEquals(true, result.getErrorMessage().contains("Simulated UDF failure"));
              return null;
            });

    pipeline.run();

    // Clean up
    Files.delete(tempFile);
  }

  @Test
  public void testUdfReturnsNullDropsDocument() throws IOException {
    // Create a temp JS file that returns null
    String jsCode =
        "function transform(inJson) {\n"
            + "  return null;\n"
            + "}";
    Path tempFile = Files.createTempFile("transform_null", ".js");
    Files.write(tempFile, jsCode.getBytes());

    Document doc = new Document("name", "John").append("age", 30);
    List<Document> inputDocs = Arrays.asList(doc);

    PCollection<Document> documents = pipeline.apply("CreateInput_Null", Create.of(inputDocs));

    PCollection<FailsafeElement<Document, String>> failsafeElements =
        documents.apply(
            "ConvertToFailsafe_Null",
            ParDo.of(new ConvertToFailsafeFn()))
            .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));

    PCollectionTuple udfResult =
        failsafeElements.apply(
            "RunUDF_Null",
            FailsafeJavascriptUdf.<Document>newBuilder()
                .setFileSystemPath(tempFile.toAbsolutePath().toString())
                .setFunctionName("transform")
                .setReloadIntervalMinutes(0)
                .setSuccessTag(SUCCESS_TAG)
                .setFailureTag(FAILURE_TAG)
                .build());

    udfResult.get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));
    udfResult.get(FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(SerializableCoder.of(Document.class), StringUtf8Coder.of()));

    PAssert.that(udfResult.get(SUCCESS_TAG)).empty();
    PAssert.that(udfResult.get(FAILURE_TAG)).empty();

    pipeline.run();

    // Clean up
    Files.delete(tempFile);
  }

  @Test
  public void testWriteBatchesFnIsolatesFailures() throws Exception {
    // Mock MongoDB classes
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDatabase = mock(MongoDatabase.class);
    MongoCollection<Document> mockCollection = mock(MongoCollection.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockDatabase.getCollection(anyString())).thenReturn(mockCollection);
    when(mockCollection.withWriteConcern(any())).thenReturn(mockCollection);

    // Create updates
    Document doc1 = new Document("_id", 1).append("name", "John");
    Document doc2 = new Document("_id", 2).append("name", "Jane");
    List<Document> docs = Arrays.asList(doc1, doc2);

    // Mock exception
    BulkWriteError error = new BulkWriteError(0, "Duplicate key", new org.bson.BsonDocument(), 0);
    MongoBulkWriteException bulkWriteException = mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(Arrays.asList(error));

    when(mockCollection.bulkWrite(anyList(), any())).thenThrow(bulkWriteException);

    TupleTag<String> failureTag = new TupleTag<String>() {};

    TupleTag<String> severeFailureTag = new TupleTag<String>() {};

    MongoDbTransforms.WriteBatchesFn fn = new MongoDbTransforms.WriteBatchesFn(
        mockClient,
        "db",
        "coll",
        false, // ordered
        "acknowledged",
        false, // journal
        1, // maxConcurrentAsyncWrites
        failureTag,
        severeFailureTag);

    fn.setup();
    fn.startBundle();
    
    DoFn<KV<String, Iterable<Document>>, Document>.ProcessContext mockProcessContext = mock(DoFn.ProcessContext.class);
    when(mockProcessContext.element()).thenReturn(KV.of("key", docs));
    
    fn.processElement(mockProcessContext);
    
    DoFn<KV<String, Iterable<Document>>, Document>.FinishBundleContext mockFinishContext = mock(DoFn.FinishBundleContext.class);
    
    fn.finishBundle(mockFinishContext);
    
    // Verify that only 1 failure was recorded (for doc1 at index 0)
    verify(mockFinishContext, times(1)).output(eq(failureTag), anyString(), any(), any());
    
    fn.teardown();
  }

  @Test
  public void testWriteBatchesFnSevereFailures() throws Exception {
    // Mock MongoDB classes
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDatabase = mock(MongoDatabase.class);
    MongoCollection<Document> mockCollection = mock(MongoCollection.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockDatabase.getCollection(anyString())).thenReturn(mockCollection);
    when(mockCollection.withWriteConcern(any())).thenReturn(mockCollection);

    // Create updates
    Document doc1 = new Document("_id", 1).append("name", "John");
    List<Document> docs = Arrays.asList(doc1);

    // Mock exception with code 2 (INVALID_ARGUMENT)
    BulkWriteError error = new BulkWriteError(2, "Invalid argument", new org.bson.BsonDocument(), 0);
    MongoBulkWriteException bulkWriteException = mock(MongoBulkWriteException.class);
    when(bulkWriteException.getWriteErrors()).thenReturn(Arrays.asList(error));

    when(mockCollection.bulkWrite(anyList(), any())).thenThrow(bulkWriteException);

    TupleTag<String> failureTag = new TupleTag<String>() {};
    TupleTag<String> severeFailureTag = new TupleTag<String>() {};

    MongoDbTransforms.WriteBatchesFn fn = new MongoDbTransforms.WriteBatchesFn(
        mockClient,
        "db",
        "coll",
        false, // ordered
        "acknowledged",
        false, // journal
        1, // maxConcurrentAsyncWrites
        failureTag,
        severeFailureTag);

    fn.setup();
    fn.startBundle();
    
    DoFn<KV<String, Iterable<Document>>, Document>.ProcessContext mockProcessContext = mock(DoFn.ProcessContext.class);
    when(mockProcessContext.element()).thenReturn(KV.of("key", docs));
    
    fn.processElement(mockProcessContext);
    
    DoFn<KV<String, Iterable<Document>>, Document>.FinishBundleContext mockFinishContext = mock(DoFn.FinishBundleContext.class);
    
    fn.finishBundle(mockFinishContext);
    
    // Verify that failure was outputted to severeFailureTag!
    verify(mockFinishContext, times(1)).output(eq(severeFailureTag), anyString(), any(), any());
    verify(mockFinishContext, times(0)).output(eq(failureTag), anyString(), any(), any());
    
    fn.teardown();
  }

  static class ConvertToFailsafeFn extends DoFn<Document, FailsafeElement<Document, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      Document d = c.element();
      c.output(FailsafeElement.of(d, d.toJson()));
    }
  }

  static class ConvertFromFailsafeFn extends DoFn<FailsafeElement<Document, String>, Document> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      FailsafeElement<Document, String> element = c.element();
      c.output(Document.parse(element.getPayload()));
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
}

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package software.amazon.qldb.tutorial.streams;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream.TRIM_HORIZON;
import static java.nio.ByteBuffer.wrap;
import static software.amazon.qldb.tutorial.Constants.LEDGER_NAME;
import static software.amazon.qldb.tutorial.Constants.STREAM_NAME;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.identitymanagement.model.PutRolePolicyRequest;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.qldb.AmazonQLDB;
import com.amazonaws.services.qldb.AmazonQLDBClientBuilder;
import com.amazonaws.services.qldb.model.CancelJournalKinesisStreamRequest;
import com.amazonaws.services.qldb.model.DescribeJournalKinesisStreamRequest;
import com.amazonaws.services.qldb.model.DescribeJournalKinesisStreamResult;
import com.amazonaws.services.qldb.model.JournalKinesisStreamDescription;
import com.amazonaws.services.qldb.model.KinesisConfiguration;
import com.amazonaws.services.qldb.model.ListJournalKinesisStreamsForLedgerRequest;
import com.amazonaws.services.qldb.model.ListJournalKinesisStreamsForLedgerResult;
import com.amazonaws.services.qldb.model.StreamJournalToKinesisRequest;
import com.amazonaws.services.qldb.model.StreamJournalToKinesisResult;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.tutorial.Constants;
import software.amazon.qldb.tutorial.CreateLedger;
import software.amazon.qldb.tutorial.CreateTable;
import software.amazon.qldb.tutorial.DeleteLedger;
import software.amazon.qldb.tutorial.DeletionProtection;
import software.amazon.qldb.tutorial.InsertDocument;
import software.amazon.qldb.tutorial.ValidateQldbHashChain;
import software.amazon.qldb.tutorial.model.SampleData;
import software.amazon.qldb.tutorial.model.streams.BlockSummaryRecord;
import software.amazon.qldb.tutorial.model.streams.Revision;
import software.amazon.qldb.tutorial.model.streams.RevisionDetailsRecord;
import software.amazon.qldb.tutorial.model.streams.StreamRecord;
import software.amazon.qldb.tutorial.qldb.JournalBlock;
import software.amazon.qldb.tutorial.qldb.QldbRevision;

/**
 * Demonstrates the QLDB stream functionality.
 * <p>
 * In this tutorial, we will create a Ledger and a stream for that Ledger. We
 * will then start a stream reader and wait for few documents to be inserted
 * that will be pulled from the Kinesis stream into the {@link #recordBuffer}
 * and the logs and for this tutorial code using the Amazon Kinesis Client
 * library
 *
 * @see <a href="https://github.com/awslabs/amazon-kinesis-client">Amazon Kinesis Client library</a>
 */
public final class StreamJournal {

    /**
     * AWS service clients used throughout the tutorial code.
     */
    public static AmazonKinesis kinesis = AmazonKinesisClientBuilder.defaultClient();
    public static AmazonIdentityManagement iam = AmazonIdentityManagementClientBuilder.defaultClient();
    public static AmazonQLDB qldb = AmazonQLDBClientBuilder.defaultClient();
    public static AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
    /**
     * Shared variables to make the tutorial code easy to follow.
     */
    public static String ledgerName = LEDGER_NAME;
    public static String streamName = STREAM_NAME;
    public static String streamId;
    public static String kdsArn;
    public static String roleArn;
    public static String kdsName;
    public static String kdsRoleName;
    public static String kdsPolicyName;
    public static Worker kdsReader;
    public static String regionName = "us-east-1";
    public static Date exclusiveEndTime;
    public static boolean isAggregationEnabled;
    public static KinesisClientLibConfiguration kclConfig;
    public static int bufferCapacity;
    public static CompletableFuture<Void> waiter;
    public static List<StreamRecord> recordBuffer = new ArrayList<>();
    public static Function<StreamRecord, Boolean> areAllRecordsFound;
    public static String STREAM_ROLE_KINESIS_STATEMENT_TEMPLATE =
            "{" +
            "   \"Sid\": \"QLDBStreamKinesisPermissions\"," +
            "   \"Action\": [\"kinesis:PutRecord\", \"kinesis:PutRecords\", " +
            "\"kinesis:DescribeStream\", \"kinesis:ListShards\"]," +
            "   \"Effect\": \"Allow\"," +
            "   \"Resource\": \"{kdsArn}\"" +
            "}";

    public static final String POLICY_TEMPLATE =
            "{" +
            "   \"Version\" : \"2012-10-17\"," +
            "   \"Statement\": [ {statements} ]" +
            "}";

    public static String ASSUME_ROLE_POLICY = POLICY_TEMPLATE.replace(
            "{statements}",
            "{" +
            "   \"Effect\": \"Allow\"," +
            "   \"Principal\": {" +
            "       \"Service\": [\"qldb.amazonaws.com\"]" +
            "   }," +
            "   \"Action\": [ \"sts:AssumeRole\" ]" +
            "}");

    private static ObjectReader reader = Constants.MAPPER.readerFor(StreamRecord.class);
    private static final Logger log = LoggerFactory.getLogger(StreamJournal.class);
    private static final int MAX_RETRIES = 60;

    private StreamJournal() { }

    /**
     * Runs the tutorial.
     *
     * @param args not required.
     */
    public static void main(final String... args) {

        initialize();

        try {
            runStreamJournalTutorial();
            log.info("You can use the AWS CLI or Console to browse the resources that were created.");
        } catch (Exception ex) {
            log.error("Something went wrong.", ex);
        } finally {
            log.info("Press Enter to clean up and exit.");
            try {
                System.in.read();
            } catch (Exception ignore) {
            }
            log.info("Starting cleanup...");
            cleanupQldbResources();
            cleanupKinesisResources();
            System.exit(0);
        }
    }

    /**
     * We will stream 24 records to Kinesis:
     * 1) Control record with CREATED status
     * 2) 2 Summary records
     * 3) 20 Revision records
     * 4) Control record with COMPLETED status (if the stream is bounded)
     *
     * @param isBounded true if the stream has an exclusive end time.
     * @return the number of expected records.
     */
    public static int numberOfExpectedRecords(boolean isBounded) {
        int baseNumber = SampleData.LICENSES.size() +
                SampleData.PEOPLE.size() +
                SampleData.REGISTRATIONS.size() +
                SampleData.VEHICLES.size() +
                // records of Block Summary
                2 +
                // CREATED Control Record
                1;

        // Bounded Stream will contain also COMPLETED Control Record
        return isBounded ? (baseNumber + 1) : baseNumber;
    }

    /**
     * Initialize the tutorial code.
     */
    public static void initialize() {
        kdsName = ledgerName + "-kinesis-stream";
        kdsRoleName = ledgerName + "-stream-role";
        kdsPolicyName = ledgerName + "-stream-policy";
        kclConfig = new KinesisClientLibConfiguration(
            ledgerName,
            kdsName,
            credentialsProvider,
            "tutorial")
            .withInitialPositionInStream(TRIM_HORIZON)
            .withRegionName(regionName);
        isAggregationEnabled = true;
        bufferCapacity = numberOfExpectedRecords(exclusiveEndTime != null);
        areAllRecordsFound = record -> recordBuffer.size() == bufferCapacity;
        waiter = new CompletableFuture<>();
    }

    /**
     * Runs the tutorial code.
     *
     * @throws Exception is thrown when something goes wrong in the tutorial.
     */
    public static void runStreamJournalTutorial() throws Exception {
        createLedger();
        createTables();
        insertDocuments();
        createQldbStream();
        startStreamReader();
        waiter.get();

        if (exclusiveEndTime != null) {
            waitForQldbStreamCompletion();
        }
        log.info("Buffered {} records so far.", recordBuffer.size());
        validateStreamRecordsHashChain();
    }

    /**
     * Creates a ledger.
     *
     * @throws Exception if the ledger creation fails.
     */
    public static void createLedger() throws Exception {
        CreateLedger.create(ledgerName);
        CreateLedger.waitForActive(ledgerName);
    }

    /**
     * Creates a few tables in the ledger created using {@link #createLedger()}.
     */
    public static void createTables() {
        CreateTable.main();
    }

    public static void insertDocuments() {
        InsertDocument.main();
    }

    /**
     * Create a QLDB Stream.
     *
     * @return the QLDB Stream description.
     * @throws InterruptedException if the thread is interrupted while waiting
     * for stream creation.
     */
    public static JournalKinesisStreamDescription createQldbStream() throws InterruptedException {
        log.info("Creating Kinesis data stream with name: '{}'...", kdsName);
        createKdsIfNotExists();
        log.info("Creating QLDB stream...");
        StreamJournalToKinesisRequest request = new StreamJournalToKinesisRequest()
                .withKinesisConfiguration(getKdsConfig())
                .withInclusiveStartTime(Date.from(Instant.now().minus(Duration.ofDays(1))))
                .withRoleArn(getOrCreateKdsRole())
                .withLedgerName(ledgerName)
                .withStreamName(streamName);

        if (exclusiveEndTime != null) {
            request = request.withExclusiveEndTime(exclusiveEndTime);
        }
        StreamJournalToKinesisResult result = qldb.streamJournalToKinesis(request);
        streamId = result.getStreamId();
        DescribeJournalKinesisStreamResult describeResult = describeQldbStream();
        log.info("Created QLDB stream: {} Current status: {}.",
                streamId,
                describeResult.getStream().getStatus());
        return describeResult.getStream();
    }

    /**
     * Create a Kinesis Data Stream to stream Journal data to Kinesis.
     */
    public static void createKdsIfNotExists() {
        try {
            log.info("Check if Kinesis Data Stream already exists.");
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest()
                    .withStreamName(kdsName);
            DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);
            log.info("Describe stream response: " + describeStreamResponse);
            kdsArn = describeStreamResponse.getStreamDescription().getStreamARN();
            String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
            if (streamStatus.equals("ACTIVE")) {
                log.info("Kinesis Data Stream is already Active.");
                return;
            }
        } catch (ResourceNotFoundException e) {
            kinesis.createStream(kdsName, 1);
        }
        waitForKdsActivation();
    }

    /**
     * Wait for Kinesis Data Stream completion.
     */
    public static void waitForQldbStreamCompletion() {
        DescribeJournalKinesisStreamRequest describeStreamRequest = new DescribeJournalKinesisStreamRequest()
                .withStreamId(streamId)
                .withLedgerName(ledgerName);

        int retries = 0;
        while (retries < MAX_RETRIES) {
            DescribeJournalKinesisStreamResult describeStreamResponse = qldb.describeJournalKinesisStream(describeStreamRequest);
            String streamStatus = describeStreamResponse.getStream().getStatus();
            log.info("Waiting for Stream Completion. Current streamStatus: {}.", streamStatus);
            if (streamStatus.equals("COMPLETED")) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception ignore) {
            }
            retries++;
        }
        if (retries >= MAX_RETRIES) {
            throw new RuntimeException("Kinesis Stream with name " + kdsName + " never went completed.");
        }
    }

    /**
     * Wait for Kinesis Data Stream activation.
     */
    private static void waitForKdsActivation() {
        log.info("Waiting for Kinesis Stream to become Active.");
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest()
                .withStreamName(kdsName);

        int retries = 0;
        while (retries < MAX_RETRIES) {
            try {
                log.info("Sleeping for 5 sec before polling Kinesis Stream status.");
                Thread.sleep(5 * 1000);
            } catch (Exception ignore) {
            }

            DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);
            kdsArn = describeStreamResponse.getStreamDescription().getStreamARN();
            String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
            if (streamStatus.equals("ACTIVE")) {
                break;
            }
            log.info("Still waiting for Kinesis Stream to become Active. Current streamStatus: {}.", streamStatus);
            try {
                Thread.sleep(1000);
            } catch (Exception ignore) {
            }
            retries++;
        }
        if (retries >= MAX_RETRIES) {
            throw new RuntimeException("Kinesis Stream with name " + kdsName + " never went active");
        }
    }

    /**
     * Create role and attaches policy to allow put records in KDS.
     *
     * @return roleArn for Kinesis Data Streams.
     * @throws InterruptedException if the thread is interrupted while creating
     * the KDS role.
     */
    public static String getOrCreateKdsRole() throws InterruptedException {
        try {
            roleArn = iam.getRole(new GetRoleRequest().withRoleName(kdsRoleName)).getRole().getArn();
            try {
                iam.getRolePolicy(new GetRolePolicyRequest().withPolicyName(kdsPolicyName).withRoleName(kdsRoleName));
            } catch (NoSuchEntityException e) {
                attachPolicyToKdsRole();
                Thread.sleep(20 * 1000);
            }
        } catch (NoSuchEntityException e) {
            log.info("The provided role doesn't exist. Creating the role with name: {}. Please wait...", kdsRoleName);
            CreateRoleRequest createRole = new CreateRoleRequest()
                    .withRoleName(kdsRoleName)
                    .withAssumeRolePolicyDocument(ASSUME_ROLE_POLICY);
            roleArn = iam.createRole(createRole).getRole().getArn();
            attachPolicyToKdsRole();
            Thread.sleep(20 * 1000);
        }
        return roleArn;
    }

    private static void attachPolicyToKdsRole() {
        String rolePolicy = POLICY_TEMPLATE
            .replace("{statements}", STREAM_ROLE_KINESIS_STATEMENT_TEMPLATE)
            .replace("{kdsArn}", kdsArn);
        PutRolePolicyRequest putPolicy = new PutRolePolicyRequest()
            .withRoleName(kdsRoleName)
            .withPolicyName(kdsPolicyName)
            .withPolicyDocument(rolePolicy);
        iam.putRolePolicy(putPolicy);
    }

    /**
     * Generate the {@link KinesisConfiguration} for the QLDB stream.
     *
     * @return {@link KinesisConfiguration} for the QLDB stream.
     */
    public static KinesisConfiguration getKdsConfig() {
        KinesisConfiguration kinesisConfiguration = new KinesisConfiguration().withStreamArn(kdsArn);

        // By default, aggregationEnabled is true so it can be specified only if needed.
        if (!isAggregationEnabled) {
            kinesisConfiguration = kinesisConfiguration.withAggregationEnabled(isAggregationEnabled);
        }
        return kinesisConfiguration;
    }

    /**
     * List QLDB streams for the ledger.
     *
     * @return map of stream Id to description for the ledger's QLDB streams.
     */
    public static Map<String, JournalKinesisStreamDescription> listQldbStreamsForLedger() {
        Map<String, JournalKinesisStreamDescription> streams = new HashMap<>();
        String nextToken = null;
        do {
            ListJournalKinesisStreamsForLedgerRequest listRequest = new ListJournalKinesisStreamsForLedgerRequest()
                    .withLedgerName(ledgerName)
                    .withNextToken(nextToken);
            ListJournalKinesisStreamsForLedgerResult listResult = qldb.listJournalKinesisStreamsForLedger(listRequest);
            listResult.getStreams().forEach(streamDescription -> streams.put(streamDescription.getStreamId(), streamDescription));
            nextToken = listResult.getNextToken();
        } while (nextToken != null);
        return streams;
    }


    /**
     * Describe the QLDB stream.
     *
     * @return description of the QLDB stream.
     */
    private static DescribeJournalKinesisStreamResult describeQldbStream() {
        DescribeJournalKinesisStreamRequest describeStreamRequest = new DescribeJournalKinesisStreamRequest()
                .withStreamId(streamId)
                .withLedgerName(ledgerName);

        return qldb.describeJournalKinesisStream(describeStreamRequest);
    }

    /**
     * Starts the stream reader using Kinesis client library.
     */
    public static void startStreamReader() {
        log.info("Starting stream reader...");

        kdsReader = new Worker.Builder()
                .recordProcessorFactory(new RevisionProcessorFactory())
                .config(kclConfig)
                .build();

        Executors.newSingleThreadExecutor().submit(() -> kdsReader.run());
    }

    /**
     * Clean up QLDB resources used by the tutorial code.
     */
    public static void cleanupQldbResources() {
        stopStreamReader();
        if (exclusiveEndTime == null) {
            cancelQldbStream();
        }
        deleteLedger();
    }

    /**
     * Cancel the QLDB stream.
     */
    public static void cancelQldbStream() {
        if (null == streamId) {
            return;
        }
        try {
            CancelJournalKinesisStreamRequest request = new CancelJournalKinesisStreamRequest()
                    .withLedgerName(ledgerName)
                    .withStreamId(streamId);
            qldb.cancelJournalKinesisStream(request);
            log.info("QLDB stream was cancelled.");
        } catch (com.amazonaws.services.qldb.model.ResourceNotFoundException ex) {
            log.info("No QLDB stream to cancel.");
        } catch (Exception ex) {
            log.warn("Error cancelling QLDB stream.", ex);
        }
    }

    /**
     * Stops the Stream Reader.
     */
    public static void stopStreamReader() {
        if (null == kdsReader) {
            return;
        }
        try {
            kdsReader.startGracefulShutdown().get(30, TimeUnit.SECONDS);
            log.info("Stream reader was stopped.");
        } catch (Exception ex) {
            log.warn("Error stopping Stream reader.", ex);
        }
    }

    public static void validateStreamRecordsHashChain() {
        List<JournalBlock> journalBlocks = streamRecordsToJournalBlocks();
        ValidateQldbHashChain.verify(journalBlocks);
    }

    private static List<JournalBlock> streamRecordsToJournalBlocks() {
        Map<ByteBuffer, QldbRevision> revisionsByHash = new HashMap<>();
        recordBuffer.stream()
            .filter(record -> record.getRecordType().equals("REVISION_DETAILS"))
            .forEach(record -> {
                try {
                    Revision revision = ((RevisionDetailsRecord) record.getPayload()).getRevision();
                    byte[] revisionHash = revision.getHash();
                    revisionsByHash.put(
                            wrap(revisionHash).asReadOnlyBuffer(),
                            QldbRevision.fromIon((IonStruct) Constants.MAPPER.writeValueAsIonValue(revision)));
                } catch (IOException e) {
                    throw new IllegalArgumentException("Could not map RevisionDetailsRecord to QldbRevision.", e);
                }
            });
        return recordBuffer.stream()
                .filter(streamRecord -> streamRecord.getRecordType().equals("BLOCK_SUMMARY"))
                .map(streamRecord -> (BlockSummaryRecord) streamRecord.getPayload())
                .distinct()
                .map(blockSummaryRecord -> blockSummaryRecordToJournalBlock(blockSummaryRecord, revisionsByHash))
                .sorted(Comparator.comparingLong(o -> o.getBlockAddress().getSequenceNo()))
                .collect(Collectors.toList());
    }

    private static JournalBlock blockSummaryRecordToJournalBlock(BlockSummaryRecord blockSummaryRecord,
                                                                 Map<ByteBuffer, QldbRevision> revisionsByHash) {
        List<QldbRevision> revisions = null;
        if (blockSummaryRecord.getRevisionSummaries() != null) {
            revisions = blockSummaryRecord.getRevisionSummaries().stream().map(revisionSummary -> {
                if (revisionSummary.getDocumentId() != null) {
                    return revisionsByHash.get(wrap(revisionSummary.getHash()).asReadOnlyBuffer());
                } else {
                    return new QldbRevision(null, null, revisionSummary.getHash(), null);
                }
            }).collect(Collectors.toList());
        }
        return new JournalBlock(
                blockSummaryRecord.getBlockAddress(),
                blockSummaryRecord.getTransactionId(),
                blockSummaryRecord.getBlockTimestamp(),
                blockSummaryRecord.getBlockHash(),
                blockSummaryRecord.getEntriesHash(),
                blockSummaryRecord.getPreviousBlockHash(),
                blockSummaryRecord.getEntriesHashList(),
                blockSummaryRecord.getTransactionInfo(),
                revisions);
    }

    /**
     * Deletes the ledger
     */
    private static void deleteLedger() {
        try {
            DeletionProtection.setDeletionProtection(ledgerName, false);
            DeleteLedger.delete(ledgerName);
            DeleteLedger.waitForDeleted(ledgerName);
            log.info("Ledger was deleted.");
        } catch (com.amazonaws.services.qldb.model.ResourceNotFoundException ex) {
            log.info("No Ledger to delete.");
        } catch (Exception ex) {
            log.warn("Error deleting Ledger.", ex);
        }
    }

    /**
     * Deletes the KDS.
     */
    public static void cleanupKinesisResources() {
        try {
            kinesis.deleteStream(kdsName);
            waitForKdsDeletion();
            log.info("KDS was deleted.");
        } catch (ResourceNotFoundException ex) {
            log.info("No KDS to delete.");
        } catch (Exception ex) {
            log.warn("Error deleting KDS.", ex);
        }
    }

    /**
     * Waits for KDS to be deleted.
     */
    private static void waitForKdsDeletion() {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(kdsName);

        int retries = 0;
        while (retries < MAX_RETRIES) {
            try {
                Thread.sleep(20 * 1000);
            } catch (Exception ignore) {
            }

            try {
                kinesis.describeStream(describeStreamRequest);
            } catch (ResourceNotFoundException ex) {
                break;
            }

            try {
                Thread.sleep(1000);
            } catch (Exception ignore) {
            }
            retries++;
        }
        if (retries >= MAX_RETRIES) {
            throw new RuntimeException("Kinesis Stream with name " + kdsName + " could not be deleted.");
        }
    }

    /**
     * Factory for {@link IRecordProcessor}s that process records from KDS.
     */
    private static class RevisionProcessorFactory implements IRecordProcessorFactory {
        @Override
        public IRecordProcessor createProcessor() {
            return new RevisionProcessor();
        }
    }

    /**
     * Processes records that show up on the KDS.
     */
    private static class RevisionProcessor implements IRecordProcessor {

        private static final Logger log = LoggerFactory.getLogger(RevisionProcessor.class);

        @Override
        public void initialize(String shardId) {
            log.info("Starting RevisionProcessor.");
        }

        @Override
        public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {
            log.info("Processing {} record(s)", records.size());
            records.forEach(r -> {
                try {
                    log.info("------------------------------------------------");
                    log.info("Processing Record with Seq: {}, PartitionKey: {}, IonText: {}",
                            r.getSequenceNumber(), r.getPartitionKey(), toIonText(r.getData()));
                    StreamRecord record = reader.readValue(r.getData().array());
                    log.info("Record Type: {}, Payload: {}.", record.getRecordType(), record.getPayload());
                    if (record.getQldbStreamArn().contains(streamId)) {
                        recordBuffer.add(record);
                        if (areAllRecordsFound.apply(record)) {
                            waiter.complete(null);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Error processing record. ", e);
                }
            });
        }

        void rewrite(byte[] data, IonWriter writer) throws IOException {
            IonReader reader = IonReaderBuilder.standard().build(data);
            writer.writeValues(reader);
        }

        private String toIonText(ByteBuffer data) throws IOException {
            StringBuilder stringBuilder = new StringBuilder();
            try (IonWriter prettyWriter = IonTextWriterBuilder.minimal().build(stringBuilder)) {
                rewrite(data.array(), prettyWriter);
            }
            return stringBuilder.toString();
        }

        @Override
        public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {
            log.info("Shutting down RevisionProcessor.");
        }
    }
}

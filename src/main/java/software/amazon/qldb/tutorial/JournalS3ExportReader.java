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

package software.amazon.qldb.tutorial;

import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.services.qldb.model.DescribeJournalS3ExportResult;
import com.amazonaws.services.qldb.model.S3ExportConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.qldb.tutorial.qldb.JournalBlock;

/**
 * Given bucket, prefix and exportId, read the contents of the export and return
 * a list of {@link JournalBlock}.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class JournalS3ExportReader {
    public static final Logger log = LoggerFactory.getLogger(JournalS3ExportReader.class);
    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    private JournalS3ExportReader() { }

    /**
     * Read the S3 export within a {@link JournalBlock}.
     *
     * @param describeJournalS3ExportResult
     *              The result from the QLDB database describing a journal export.
     * @param amazonS3
     *              The low level S3 client.
     * @return a list of {@link JournalBlock}.
     */
    public static List<JournalBlock> readExport(final DescribeJournalS3ExportResult describeJournalS3ExportResult,
        final AmazonS3 amazonS3) {

        S3ExportConfiguration exportConfiguration =
            describeJournalS3ExportResult.getExportDescription().getS3ExportConfiguration();

        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
            .withBucketName(exportConfiguration.getBucket())
            .withPrefix(exportConfiguration.getPrefix());

        ListObjectsV2Result listObjectsV2Result = amazonS3.listObjectsV2(listObjectsRequest);
        log.info("Found the following objects for list from s3: ");
        listObjectsV2Result.getObjectSummaries()
            .forEach(s3ObjectSummary -> log.info(s3ObjectSummary.getKey()));

        // Validate initial manifest file was written.
        String expectedManifestKey = exportConfiguration.getPrefix() +
                describeJournalS3ExportResult.getExportDescription().getExportId() + ".started" + ".manifest";
        String initialManifestKey = listObjectsV2Result
            .getObjectSummaries()
            .stream()
            .filter(s3ObjectSummary -> s3ObjectSummary.getKey().equalsIgnoreCase(expectedManifestKey))
            .map(S3ObjectSummary::getKey)
            .findFirst().orElseThrow(() -> new IllegalStateException("Initial manifest not found."));

        log.info("Found the initial manifest with key " + initialManifestKey);

        // Find the final manifest file, it should contain the exportId in it.
        String completedManifestFileKey = listObjectsV2Result
            .getObjectSummaries()
            .stream()
            .filter(s3ObjectSummary -> s3ObjectSummary.getKey().endsWith("completed.manifest")
                    && (s3ObjectSummary
                    .getKey()
                    .contains(describeJournalS3ExportResult.getExportDescription().getExportId())))
            .map(S3ObjectSummary::getKey)
            .findFirst().orElseThrow(() -> new IllegalStateException("Completed manifest not found."));

        log.info("Found the completed manifest with key " + completedManifestFileKey);

        // Read manifest file to find data file keys.
        S3Object completedManifestObject = amazonS3.getObject(exportConfiguration.getBucket(), completedManifestFileKey);

        List<String> dataFileKeys = getDataFileKeysFromManifest(completedManifestObject);

        log.info("Found the following keys in the manifest files: " + dataFileKeys);

        List<JournalBlock> journalBlocks = new ArrayList<>();
        for (String key : dataFileKeys) {
            log.info("Reading file with S3 key " + key + " from bucket: " + exportConfiguration.getBucket());
            S3Object s3Object = amazonS3.getObject(exportConfiguration.getBucket(), key);
            List<JournalBlock> blocks = getJournalBlocks(s3Object);
            compareKeyWithContentRange(key, blocks.get(0), blocks.get(blocks.size() - 1));
            journalBlocks.addAll(blocks);
        }
        return journalBlocks;
    }

    /**
     * Compares the expected block range, derived from File Key, with the actual object content.
     *
     * @param fileKey
     *              The key of data file containing the chunk of {@link JournalBlock}.
     *              The fileKey pattern is {@code [strandId].[firstSequenceNo]-[lastSequenceNo].ion}.
     * @param firstBlock
     *              The first block decoded from the object content.
     * @param lastBlock
     *              The last block decoded from the object content.
     * @throws IllegalStateException if either of the {@link JournalBlock}s' sequenceNo does not match the expected number.
     */
    private static void compareKeyWithContentRange(final String fileKey, final JournalBlock firstBlock,
                                                   final JournalBlock lastBlock) {
        // the key pattern is [strandId].[firstSequenceNo]-[lastSequenceNo].ion
        String sequenceNoRange = fileKey.split("\\.")[1];
        String[] keyTokens = sequenceNoRange.split("-");
        long startSequenceNo = Long.valueOf(keyTokens[0]);
        long lastSequenceNo = Long.valueOf(keyTokens[1]);

        // compare the first sequenceNo of the fileKey to the sequenceNo of the first block.
        // block address is [strandId]/[sequenceNo]
        if (firstBlock.getBlockAddress().getSequenceNo() != startSequenceNo) {
            throw new IllegalStateException("Expected first block SequenceNo to be " + startSequenceNo);
        }

        // compare the second sequenceNo of the fileKey to the sequenceNo of the last block.
        if (lastBlock.getBlockAddress().getSequenceNo() != lastSequenceNo) {
            throw new IllegalStateException("Expected last block SequenceNo to be " + lastSequenceNo);
        }
    }

    /**
     * Retrieve a list of {@link JournalBlock} from the given {@link S3Object}.
     *
     * @param s3Object
     *              A {@link S3Object}.
     * @return a list of {@link JournalBlock}.
     * @throws IllegalStateException if invalid IonType is found in the S3 Object.
     */
    private static List<JournalBlock> getJournalBlocks(final S3Object s3Object) {
        IonReader ionReader = SYSTEM.newReader(s3Object.getObjectContent());
        List<JournalBlock> journalBlocks = new ArrayList<>();
        // data files contain list of blocks
        while (ionReader.next() != null) {
            if (ionReader.getType() != IonType.STRUCT) {
                throw new IllegalStateException("Expected ion STRUCT but found " + ionReader.getType());
            }
            try {
                journalBlocks.add(Constants.MAPPER.readValue(SYSTEM.newValue(ionReader), JournalBlock.class));
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }
        log.info("Found " + journalBlocks.size() + " blocks(s) from data file - " + s3Object.getKey());
        return journalBlocks;
    }

    /**
     * Given the S3Object to the completed manifest file, return the keys
     * which are part of this export request.
     *
     * @param s3Object
     *              A {@link S3Object}.
     * @return a list of data file keys containing the chunk of {@link JournalBlock}.
     */
    private static List<String> getDataFileKeysFromManifest(final S3Object s3Object) {
        IonReader ionReader = IonReaderBuilder.standard().build(s3Object.getObjectContent());
        ionReader.next(); // Read the data
        List<String> keys = new ArrayList<>();
        IonStruct ionStruct = (IonStruct) SYSTEM.newValue(ionReader);
        IonList ionKeysList = (IonList) ionStruct.get("keys");
        ionKeysList.forEach(key -> keys.add(((IonString) key).stringValue()));
        return keys;
    }
}

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

import com.amazonaws.services.qldb.AmazonQLDB;
import com.amazonaws.services.qldb.AmazonQLDBClientBuilder;
import com.amazonaws.services.qldb.model.DescribeJournalS3ExportRequest;
import com.amazonaws.services.qldb.model.DescribeJournalS3ExportResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describe a specific journal export with the given ExportId.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class DescribeJournalExport {
    public static final Logger log = LoggerFactory.getLogger(DescribeJournalExport.class);
    public static AmazonQLDB client = getClient();

    private DescribeJournalExport() { }

    public static AmazonQLDB getClient() {
        return AmazonQLDBClientBuilder.standard().build();
    }

    public static void main(final String... args) throws Exception {
        String exportId;
        if (args.length != 1) {
            throw new IllegalAccessException("Missing ExportId argument in DescribeJournalExport.");
        }
        exportId = args[0];

        log.info("Running describe export journal tutorial with ExportId: " + exportId);

        try {
            describeExport(Constants.LEDGER_NAME, exportId);
        } catch (Exception e) {
            log.error("Unable to describe an export!", e);
            throw e;
        }
    }

    /**
     * Describe a journal export.
     *
     * @param ledgerName
     *              The ledger from which the journal is being exported.
     * @param exportId
     *              Unique ExportID of the journal export.
     * @return {@link DescribeJournalS3ExportResult} from QLDB.
     */
    public static DescribeJournalS3ExportResult describeExport(String ledgerName, String exportId) {
        log.info("Let's describe a journal export for ledger with name: {}, ExportId: {}...", ledgerName, exportId);

        DescribeJournalS3ExportRequest request = new DescribeJournalS3ExportRequest()
                .withName(ledgerName)
                .withExportId(exportId);

        DescribeJournalS3ExportResult result = client.describeJournalS3Export(request);
        log.info("Export described. result = " + result);
        return result;
    }
}

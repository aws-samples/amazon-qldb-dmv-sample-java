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
import com.amazonaws.services.qldb.model.JournalS3ExportDescription;
import com.amazonaws.services.qldb.model.ListJournalS3ExportsForLedgerRequest;
import com.amazonaws.services.qldb.model.ListJournalS3ExportsForLedgerResult;
import com.amazonaws.services.qldb.model.ListJournalS3ExportsRequest;
import com.amazonaws.services.qldb.model.ListJournalS3ExportsResult;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * List the journal exports of a given QLDB ledger.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class ListJournalExports {
    public static AmazonQLDB client = getClient();
    public static final Logger log = LoggerFactory.getLogger(DescribeJournalExport.class);

    private ListJournalExports() { }

    public static AmazonQLDB getClient() {
        return AmazonQLDBClientBuilder.standard().build();
    }

    /**
     * List all journal exports for the given ledger.
     *
     * @param name
     *              The name of the ledger.
     * @return a list of {@link JournalS3ExportDescription}.
     */
    public static List<JournalS3ExportDescription> listExports(final String name) {
        log.info("Let's list journal exports for the ledger with name: {}...", name);

        List<JournalS3ExportDescription> exportDescriptions = new ArrayList<>();
        String nextToken = null;
        do {
            ListJournalS3ExportsForLedgerRequest request = new ListJournalS3ExportsForLedgerRequest()
                    .withName(name)
                    .withNextToken(nextToken);
            ListJournalS3ExportsForLedgerResult result = client.listJournalS3ExportsForLedger(request);
            exportDescriptions.addAll(result.getJournalS3Exports());
            nextToken = result.getNextToken();
        } while (nextToken != null);

        log.info("Success. List of journal exports: {}", exportDescriptions);
        return exportDescriptions;
    }

    /**
     *List all journal exports for the given ledger and nextToken.
     *
     * @param name
     *              The name of the ledger.
     * @param nextToken
     *              The next token to provide in the service call.
     * @return a list of {@link JournalS3ExportDescription}.
     */
    public static List<JournalS3ExportDescription> listExports(final String name, final String nextToken) {
        log.info("Let's list journal exports for the ledger with name: {}, nextToken: {}...", name, nextToken);

        ListJournalS3ExportsForLedgerRequest request = new ListJournalS3ExportsForLedgerRequest()
                .withName(name)
                .withNextToken(nextToken);
        ListJournalS3ExportsForLedgerResult result = client.listJournalS3ExportsForLedger(request);
        List<JournalS3ExportDescription> exportDescriptions = result.getJournalS3Exports();

        log.info("Success. List of journal exports: {}", exportDescriptions);
        return exportDescriptions;
    }

    /**
     * List all journal exports for an AWS account.
     *
     * @return a list of {@link JournalS3ExportDescription}.
     */
    public static List<JournalS3ExportDescription> listExports() {
        log.info("Let's list journal exports for the AWS account.");

        List<JournalS3ExportDescription> exportDescriptions = new ArrayList<>();
        String nextToken = null;
        do {
            ListJournalS3ExportsRequest request = new ListJournalS3ExportsRequest()
                    .withNextToken(nextToken);
            ListJournalS3ExportsResult result = client.listJournalS3Exports(request);
            exportDescriptions.addAll(result.getJournalS3Exports());
            nextToken = result.getNextToken();
        } while (nextToken != null);

        log.info("Success. List of journal exports: {}", exportDescriptions);
        return exportDescriptions;
    }

    public static void main(final String... args) throws Exception {
        try {
            listExports(Constants.LEDGER_NAME);
        } catch (Exception e) {
            log.error("Unable to list exports!", e);
            throw e;
        }
    }
}

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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonStruct;

import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;

/**
 * Scan for all the documents in a table.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class ScanTable {
    private static final Logger log = LoggerFactory.getLogger(ScanTable.class);

    private ScanTable() { }

    /**
     * Scan the table with the given {@code tableName} for all documents.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param tableName
     *              Name of the table to scan.
     * @return a list of documents in {@link IonStruct} .
     */
    public static List<IonStruct> scanTableForDocuments(final TransactionExecutor txn, final String tableName) {
        log.info("Scanning '{}'...", tableName);
        final String scanTable = String.format("SELECT * FROM %s", tableName);
        List<IonStruct> documents = toIonStructs(txn.execute(scanTable));
        log.info("Scan successful!");
        printDocuments(documents);
        return documents;
    }

    /**
     * Pretty print all elements in the provided {@link Result}.
     *
     * @param result
     *              {@link Result} from executing a query.
     */
    public static void printDocuments(final Result result) {
        result.iterator().forEachRemaining(row -> log.info(row.toPrettyString()));
    }

    /**
     * Pretty print all elements in the provided list of {@link IonStruct}.
     *
     * @param documents
     *              List of documents to print.
     */
    public static void printDocuments(final List<IonStruct> documents) {
        documents.forEach(row -> log.info(row.toPrettyString()));
    }

    /**
     * Convert the result set into a list of {@link IonStruct}.
     *
     * @param result
     *              {@link Result} from executing a query.
     * @return a list of documents in IonStruct.
     */
    public static List<IonStruct> toIonStructs(final Result result) {
        final List<IonStruct> documentList = new ArrayList<>();
        result.iterator().forEachRemaining(row -> documentList.add((IonStruct) row));
        return documentList;
    }

    public static void main(final String... args) {
        ConnectToLedger.getDriver().execute(txn -> {
            List<String> tableNames = scanTableForDocuments(txn, Constants.USER_TABLES)
                .stream()
                .map((s) -> s.get("name").toString())
                .collect(Collectors.toList());
            for (String tableName : tableNames) {
                scanTableForDocuments(txn, tableName);
            }
        });
    }
}

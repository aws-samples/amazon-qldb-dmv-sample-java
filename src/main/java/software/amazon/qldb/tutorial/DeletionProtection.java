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
import com.amazonaws.services.qldb.model.CreateLedgerRequest;
import com.amazonaws.services.qldb.model.CreateLedgerResult;
import com.amazonaws.services.qldb.model.PermissionsMode;
import com.amazonaws.services.qldb.model.ResourcePreconditionNotMetException;
import com.amazonaws.services.qldb.model.UpdateLedgerRequest;
import com.amazonaws.services.qldb.model.UpdateLedgerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrate the protection of QLDB ledgers against deletion.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public class DeletionProtection {
    public static final Logger log = LoggerFactory.getLogger(DeletionProtection.class);

    public static final String LEDGER_NAME = "tag-demo";

    public static AmazonQLDB client = CreateLedger.getClient();

    private DeletionProtection() {}

    public static void main(String... args) throws Exception {
        try {
            createWithDeletionProtection(LEDGER_NAME);

            CreateLedger.waitForActive(LEDGER_NAME);

            setDeletionProtection(LEDGER_NAME, true);

            try {
                DeleteLedger.delete(LEDGER_NAME);
            } catch (ResourcePreconditionNotMetException e) {
                log.info("Ledger protected against deletions!");
            }

            setDeletionProtection(LEDGER_NAME, false);

            DeleteLedger.delete(LEDGER_NAME);
        } catch (Exception e) {
            log.error("Error while updating or deleting the ledger!", e);
            throw e;
        }
    }

    public static CreateLedgerResult createWithDeletionProtection(String ledgerName) {
        log.info("Let's create the ledger with name: {}...", ledgerName);
        CreateLedgerRequest request = new CreateLedgerRequest()
                .withName(ledgerName)
                .withPermissionsMode(PermissionsMode.ALLOW_ALL)
                .withDeletionProtection(true);
        CreateLedgerResult result = client.createLedger(request);
        log.info("Success. Ledger state: {}", result.getState());
        return result;
    }

    public static UpdateLedgerResult setDeletionProtection(String ledgerName, boolean deletionProtection) {
        log.info("Let's set deletionProtection to {} for the ledger with name {}", deletionProtection, ledgerName);
        UpdateLedgerRequest request = new UpdateLedgerRequest()
                .withName(ledgerName)
                .withDeletionProtection(deletionProtection);

        UpdateLedgerResult result = client.updateLedger(request);
        log.info("Success. Ledger updated: {}", result);
        return result;
    }
}

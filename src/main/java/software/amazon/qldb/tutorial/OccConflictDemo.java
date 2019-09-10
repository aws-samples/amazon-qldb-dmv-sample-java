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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonValue;
import com.amazonaws.services.qldbsession.model.OccConflictException;

import software.amazon.qldb.QldbSession;
import software.amazon.qldb.Transaction;
import software.amazon.qldb.tutorial.model.SampleData;

/**
 * Demonstrates how to handle OCC conflicts, where two users try to execute and commit changes to the same document.
 * When OCC conflict occurs on execute or commit, implicitly handled by restarting the transaction.
 * In this example, two sessions on the same ledger try to access the registration city for the same Vehicle Id.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class OccConflictDemo {
    public static final Logger log = LoggerFactory.getLogger(OccConflictDemo.class);

    private OccConflictDemo() { }

    /**
     * Commit the transaction and retry up to a constant number of times.
     *
     * @param qldbSession
     *              A QLDB session.
     * @param transaction
     *              An open transaction.
     * @param query
     *              The query to execute.
     * @param parameters
     *              The parameters for the query.
     * @throws IOException if failed to convert parameter into an {@link IonValue}.
     */
    public static void commitTransaction(final QldbSession qldbSession, Transaction transaction, final String query,
                                         final String parameters)
            throws IOException {
        for (int i = 0; i < Constants.RETRY_LIMIT; i++) {
            try {
                transaction.commit();
                log.info("Commit successful after {} retries.", i);
                break;
            } catch (OccConflictException e) {
                log.info("Commit failed due to an OCC conflict. Restart transaction.");
                transaction = qldbSession.startTransaction();
                executeTransaction(qldbSession, transaction, query, parameters);
            }
        }
    }

    /**
     * Start a new transaction and execute it with the given statement.
     *
     * @param qldbSession
     *              A QLDB session.
     * @param transaction
     *              An open transaction.
     * @param query
     *              The query to execute.
     * @param parameters
     *              The parameters for the query.
     * @throws IOException if failed to convert parameter into an IonValue.
     */
    public static void executeTransaction(final QldbSession qldbSession, Transaction transaction, final String query,
                                          final String parameters)
            throws IOException {
        for (int i = 0; i < Constants.RETRY_LIMIT; i++) {
            try {
                final List<IonValue> parameter = Collections.singletonList(Constants.MAPPER.writeValueAsIonValue(parameters));
                transaction.execute(query, parameter);
                log.info("Execute successful after {} retries.", i);
                break;
            } catch (OccConflictException e) {
                log.info("Execute on qldbSession failed due to an OCC conflict. Restart transaction.");
                transaction = qldbSession.startTransaction();
            }
        }
    }

    public static void main(final String... args) throws Exception {
        try (QldbSession qldbSession1 = ConnectToLedger.createQldbSession();
             QldbSession qldbSession2 = ConnectToLedger.createQldbSession()) {
            final String query1 = "UPDATE VehicleRegistration AS r SET r.City = 'Tukwila' WHERE r.VIN = ?";
            final String query2 = "SELECT * FROM VehicleRegistration AS r WHERE r.VIN = ?";
            final String vehicleId1 = SampleData.REGISTRATIONS.get(0).getVin();
            final String vehicleId2 = SampleData.REGISTRATIONS.get(0).getVin();

            log.info("Updating the registration city on transaction 1...");
            final Transaction t1 = qldbSession1.startTransaction();
            log.info("Selecting the registrations on transaction 2...");
            final Transaction t2 = qldbSession2.startTransaction();

            log.info("Executing transaction 1...");
            executeTransaction(qldbSession1, t1, query1, vehicleId1);
            log.info("Executing transaction 2...");
            executeTransaction(qldbSession2, t2, query2, vehicleId2);

            log.info("Committing transaction 1...");
            commitTransaction(qldbSession1, t1, query1, vehicleId1);

            // The first attempt to commit on t2 will fail due to an OCC conflict.
            log.info("Committing transaction 2...");
            commitTransaction(qldbSession2, t2, query2, vehicleId2);
        }
    }
}

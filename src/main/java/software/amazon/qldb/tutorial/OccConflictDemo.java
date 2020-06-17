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

import com.amazon.ion.IonValue;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.qldbsession.model.OccConflictException;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.RetryPolicy;
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

    public static void main(final String... args) throws IOException {
        final String vehicleId1 = SampleData.REGISTRATIONS.get(0).getVin();
        final IonValue ionVin = Constants.MAPPER.writeValueAsIonValue(vehicleId1);

        QldbDriver driver = QldbDriver.builder()
                                              .ledger(Constants.LEDGER_NAME)
                                              .transactionRetryPolicy(RetryPolicy.none())
                                              .sessionClientBuilder(ConnectToLedger.getAmazonQldbSessionClientBuilder())
                                              .build();

        /*
         * ⚠️Warning⚠️: Running a transaction inside another transaction that access the same document
         * is not recommended at all.
         *
         * The below code will always have an Optimistic Concurrency Control problem because the inner
         * transaction modified that same document than the outer transaction accessed when it started
         * the transaction.
         */
        try {
            log.info("Starting outer transaction");
            driver.execute(txn -> {
                txn.execute("UPDATE VehicleRegistration AS r SET r.City = 'Tukwila' WHERE r.VIN = ?",
                            ionVin);
                txn.execute("SELECT * FROM VehicleRegistration AS r WHERE r.VIN = ?",
                            ionVin);

                driver.execute(txn2 -> {
                    log.info("Starting inner transaction");
                    txn2.execute("UPDATE VehicleRegistration AS r SET r.City = 'Tukwila' WHERE r.VIN = ?",
                                ionVin);
                    txn2.execute("SELECT * FROM VehicleRegistration AS r WHERE r.VIN = ?",
                                ionVin);
                });
                log.info("Inner transaction succeeded and the Vehicle with VIN {} was updated", vehicleId1);
            });
        } catch (OccConflictException e) {
            log.error("Optimistic Concurrency Control Exception. One of the threads tried to commit the transaction that used"
                     + "the same "
                     + "thread already.");
        }
    }
}

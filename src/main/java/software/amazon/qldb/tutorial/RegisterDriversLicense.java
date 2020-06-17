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
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;

import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;
import software.amazon.qldb.tutorial.model.DriversLicense;
import software.amazon.qldb.tutorial.model.Person;
import software.amazon.qldb.tutorial.model.SampleData;

/**
 * Register a new driver's license.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class RegisterDriversLicense {
    public static final Logger log = LoggerFactory.getLogger(RegisterDriversLicense.class);

    private RegisterDriversLicense() { }

    /**
     * Verify whether a driver already exists in the database.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param govId
     *              The government ID of the new owner.
     * @return {@code true} if the driver has already been registered; {@code false} otherwise.
     * @throws IOException if failed to convert parameter into an IonValue.
     */
    public static boolean personAlreadyExists(final TransactionExecutor txn, final String govId) throws IOException {
        final String query = "SELECT * FROM Person AS p WHERE p.GovId = ?";

        final Result result = txn.execute(query, Constants.MAPPER.writeValueAsIonValue(govId));
        return !result.isEmpty();
    }

    /**
     * Verify whether a driver has a driver's license in the database.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param personId
     *              The unique personId of the new owner.
     * @return {@code true} if driver has a driver's license; {@code false} otherwise.
     * @throws IOException if failed to convert parameter into an IonValue.
     */
    public static boolean personHadDriversLicense(final TransactionExecutor txn, final String personId)
            throws IOException {
        Result result = queryDriversLicenseByPersonId(txn, personId);
        return !result.isEmpty();
    }

    /**
     * Find a driver's license using the given personId.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param personId
     *              The unique personId of a driver.
     * @return the result set.
     * @throws IOException if failed to convert parameter into an IonValue.
     */
    public static Result queryDriversLicenseByPersonId(final TransactionExecutor txn, final String personId)
            throws IOException {
        final String query = "SELECT * FROM DriversLicense AS d WHERE d.PersonId = ?";
        final List<IonValue> parameters = Collections.singletonList(Constants.MAPPER.writeValueAsIonValue(personId));
        return txn.execute(query, parameters);
    }

    /**
     * Register a new driver's license.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param govId
     *              The government ID of the new owner.
     * @param license
     *              The new license to register.
     * @param personId
     *              The unique personId of the new owner.
     * @throws IllegalStateException if failed to convert document ID to an IonValue.
     */
    public static void registerNewDriversLicense(final TransactionExecutor txn, final String govId,
                                                 final DriversLicense license, final String personId) {
        try {
            if (personHadDriversLicense(txn, personId)) {
                log.info("Person with government ID '{}' already has a license! No new license added.", govId);
                return;
            }

            final String query = "INSERT INTO DriversLicense ?";
            log.info(new IonObjectMapper().writeValueAsIonValue(license).toPrettyString());
            final List<IonValue> parameters = Collections.singletonList(Constants.MAPPER.writeValueAsIonValue(license));
            txn.execute(query, parameters);

            Result result = queryDriversLicenseByPersonId(txn, govId);
            if (ScanTable.toIonStructs(result).size() > 0) {
                log.info("Problem occurred while inserting new license, please review the results.");
            } else {
                log.info("Successfully registered new driver.");
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    public static void main(final String... args) {
        final Person newPerson = new Person(
            "Kate",
            "Mulberry",
            SampleData.convertToLocalDate("1995-02-09"),
            "AQQ17B2342",
            "Passport",
            "22 Commercial Drive, Blaine, WA, 97722"
        );
        ConnectToLedger.getDriver().execute(txn -> {
            final String documentId;
            final List<String> documentIdList;
            try {
                if (personAlreadyExists(txn, newPerson.getGovId())) {
                    log.info("Person with this GovId already exists.");
                    documentId = Person.getDocumentIdByGovId(txn, newPerson.getGovId());
                } else {
                    documentIdList = InsertDocument.insertDocuments(txn, Constants.PERSON_TABLE_NAME,
                            Collections.singletonList(newPerson));
                    documentId = documentIdList.get(0);
                }
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }

            final DriversLicense newLicense = new DriversLicense(
                    documentId,
                    "112 360 PXJ",
                    "Full",
                    SampleData.convertToLocalDate("2018-06-30"),
                    SampleData.convertToLocalDate("2022-10-30")
            );

            registerNewDriversLicense(txn, newPerson.getGovId(), newLicense, documentId);
        });
    }
}

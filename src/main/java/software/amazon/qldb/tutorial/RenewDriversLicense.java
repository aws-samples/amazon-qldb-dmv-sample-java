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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazon.ion.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;

import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;
import software.amazon.qldb.tutorial.model.Owner;
import software.amazon.qldb.tutorial.model.SampleData;

/**
 * Find the person associated with a license number.
 * Renew a driver's license.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class RenewDriversLicense {
    public static final Logger log = LoggerFactory.getLogger(RegisterDriversLicense.class);
    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    private RenewDriversLicense() { }

    /**
     * Get the PersonId of a driver's license using the given license number.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param licenseNumber
     *              License number of the driver's license to query.
     * @return the PersonId.
     * @throws IllegalStateException if failed to convert parameter into an {@link IonValue}, or
     *                          if no PersonId was found.
     */
    public static String getPersonIdFromLicenseNumber(final TransactionExecutor txn, final String licenseNumber) {
        try {
            log.info("Finding person ID with license number: {}...", licenseNumber);
            final String query = "SELECT PersonId FROM DriversLicense WHERE LicenseNumber = ?";

            final Result result = txn.execute(query, Constants.MAPPER.writeValueAsIonValue(licenseNumber));
            if (result.isEmpty()) {
                ScanTable.printDocuments(result);
                throw new IllegalStateException("Unable to find person with license number: " + licenseNumber);
            }
            return Constants.MAPPER.readValue(result.iterator().next(), Owner.class).getPersonId();
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    /**
     * Find a driver using the given personId.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param personId
     *              The unique personId of a driver.
     * @throws IllegalStateException if failed to convert parameter into an {@link IonValue}, or
     *                          if no driver was found using the given license number.
     */
    public static void verifyDriverFromLicenseNumber(final TransactionExecutor txn, final String personId) {
        try {
            log.info("Finding person with person ID: {}...", personId);
            final String query = "SELECT p.* FROM Person AS p BY pid WHERE pid = ?";
            final List<IonValue> parameters = Collections.singletonList(Constants.MAPPER.writeValueAsIonValue(personId));
            Result result = txn.execute(query, parameters);
            if (result.isEmpty()) {
                ScanTable.printDocuments(result);
                throw new IllegalStateException("Unable to find person with ID: " + personId);
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    /**
     * Renew the ValidToDate and ValidFromDate of a driver's license.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param licenseNumber
     *              License number of the driver's license to update.
     * @param validFromDate
     *              The new ValidFromDate.
     * @param validToDate
     *              The new ValidToDate.
     * @return the list of updated Document IDs.
     * @throws IllegalStateException if failed to convert parameter into an {@link IonValue}.
     */
    public static List<String> renewDriversLicense(final TransactionExecutor txn, final String licenseNumber,
                                                   final LocalDate validFromDate, final LocalDate validToDate) {
        try {
            log.info("Renewing license with license number: {}...", licenseNumber);
            final String query = "UPDATE DriversLicense AS d SET d.ValidFromDate = ?, d.ValidToDate = ? "
                + "WHERE d.LicenseNumber = ?";
            final List<IonValue> parameters = new ArrayList<>();
            parameters.add(localDateToTimestamp(validFromDate));
            parameters.add(localDateToTimestamp(validToDate));
            parameters.add(Constants.MAPPER.writeValueAsIonValue(licenseNumber));
            Result result = txn.execute(query, parameters);
            List<String> list = SampleData.getDocumentIdsFromDmlResult(result);
            log.info("DriversLicense Document IDs which had licenses renewed: ");
            list.forEach(log::info);
            return list;
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    public static void main(final String... args) {
        final String licenseNumber = SampleData.LICENSES.get(0).getLicenseNumber();
        ConnectToLedger.getDriver().execute(txn -> {
            final String personId = getPersonIdFromLicenseNumber(txn, licenseNumber);
            verifyDriverFromLicenseNumber(txn, personId);
            renewDriversLicense(txn, licenseNumber,
                    SampleData.convertToLocalDate("2019-04-19"), SampleData.convertToLocalDate("2023-04-19"));
        });
    }

    private static IonValue localDateToTimestamp(LocalDate date) {
        return SYSTEM.newTimestamp(Timestamp.forDay(date.getYear(), date.getMonthValue(), date.getDayOfMonth()));
    }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonValue;

import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;
import software.amazon.qldb.tutorial.model.DriversLicense;
import software.amazon.qldb.tutorial.model.SampleData;

/**
 * De-register a driver's license.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class DeregisterDriversLicense {
    public static final Logger log = LoggerFactory.getLogger(DeregisterDriversLicense.class);

    private DeregisterDriversLicense() { }

    /**
     * De-register a driver's license specified by the given license number.
     *
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param licenseNumber
     *              License number of the driver's license to de-register.
     * @throws IllegalStateException if failed to convert parameter into an {@link IonValue}.
     */
    public static void deregisterDriversLicense(final TransactionExecutor txn, final String licenseNumber) {
        try {
            log.info("De-registering license with license number: {}...", licenseNumber);
            final String query = "DELETE FROM DriversLicense AS d WHERE d.LicenseNumber = ?";

            final Result result = txn.execute(query, Constants.MAPPER.writeValueAsIonValue(licenseNumber));
            if (!result.isEmpty()) {
                log.info("Successfully de-registered license: {}.", licenseNumber);
            } else {
                log.error("Error de-registering license, license {} not found.", licenseNumber);
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    public static void main(final String... args) {
        final DriversLicense license = SampleData.LICENSES.get(1);
        ConnectToLedger.getDriver().execute(txn -> {
            deregisterDriversLicense(txn, license.getLicenseNumber());
        });
    }
}

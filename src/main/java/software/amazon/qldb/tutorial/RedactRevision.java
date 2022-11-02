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

import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazonaws.services.qldb.AmazonQLDB;
import com.amazonaws.services.qldb.model.GetDigestResult;
import com.amazonaws.services.qldb.model.GetRevisionResult;
import com.amazonaws.services.qldb.model.ValueHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;
import software.amazon.qldb.tutorial.model.Person;
import software.amazon.qldb.tutorial.model.SampleData;
import software.amazon.qldb.tutorial.model.VehicleRegistration;
import software.amazon.qldb.tutorial.qldb.BlockAddress;
import software.amazon.qldb.tutorial.qldb.QldbRevision;
import software.amazon.qldb.tutorial.qldb.RedactRevisionRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;

import static software.amazon.qldb.tutorial.Constants.LEDGER_NAME;
import static software.amazon.qldb.tutorial.Constants.USER_TABLES;
import static software.amazon.qldb.tutorial.Constants.VEHICLE_REGISTRATION_TABLE_NAME;
import static software.amazon.qldb.tutorial.ScanTable.printDocuments;
import static software.amazon.qldb.tutorial.ScanTable.toIonStructs;

/**
 * Transfer the ownership of a particular vehicle's registration to a new
 * owner and redact the previous revision of the registration. In addition,
 * also verify the integrity of the document after the redaction is complete.
 */
public final class RedactRevision {
    public static final Logger log = LoggerFactory.getLogger(RedactRevision.class);
    public static AmazonQLDB client = CreateLedger.getClient();

    private RedactRevision() {
    }

    /**
     * Get the unique table ID for the given table name.
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param tableName
     *              The table name for which table ID is returned.
     * @return
     *              The unique table ID for the given table name.
     */
    public static String getTableId(final TransactionExecutor txn, final String tableName) {
        return ScanTable.scanTableForDocuments(txn, USER_TABLES)
            .stream()
            .filter((s) -> ((IonString) s.get("name")).stringValue().equals(tableName))
            .map((s) -> ((IonString) s.get("tableId")).stringValue())
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(String.format("Unable to find table with name: %s", tableName)));
    }

    /**
     * Get the previous revision of a vehicle registration by querying the
     * VehicleRegistration table's history for the previous owner's PersonId.
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param registrationDocumentId
     *              The unique document ID of the specified vehicle registration.
     * @param ownerDocumentId
     *              The unique document ID of the vehicle's previous owner.
     * @return
     *              The historic {@link QldbRevision} of the vehicle registration
     *              that has the previous owner as the primary owner.
     */
    public static QldbRevision historicRegistration(
        final TransactionExecutor txn,
        final String registrationDocumentId,
        final String ownerDocumentId
    ) {
        try {
            final String historyQuery = "SELECT * FROM history(VehicleRegistration) AS h "
                + "WHERE h.metadata.id = ? AND h.data.Owners.PrimaryOwner.PersonId = ?";
            log.info(
                "Querying the 'VehicleRegistration' table's history for a registration with documentId: {} and owner: {}",
                registrationDocumentId, ownerDocumentId
            );
            final Result result = txn.execute(
                historyQuery,
                Constants.MAPPER.writeValueAsIonValue(registrationDocumentId),
                Constants.MAPPER.writeValueAsIonValue(ownerDocumentId)
            );
            final List<IonStruct> revisions = toIonStructs(result);
            if (revisions.size() == 0) {
                throw new IllegalStateException(String.format(
                    "Unable to find a historic registration with documentId: %s and owner: %s",
                    registrationDocumentId,
                    ownerDocumentId
                ));
            } else if (revisions.size() > 1) {
                throw new IllegalStateException(String.format(
                    "Found more than 1 historic registrations with documentId: %s and owner: %s",
                    registrationDocumentId,
                    ownerDocumentId
                ));
            }
            printDocuments(revisions);
            return QldbRevision.fromIon(revisions.get(0));
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    /**
     * Get the historic revision for a document at the given block address.
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param tableName
     *              The table name where the document exists.
     * @param documentId
     *              The unique ID of the document revision.
     * @param version
     *              The {@link BlockAddress} that contains the historic revision.
     * @return
     *              The {@link QldbRevision} for the document at the given block address.
     */
    public static QldbRevision queryHistoryForRevisionByVersion(
        TransactionExecutor txn,
        String tableName,
        String documentId,
        long version
    ) {
        try {
            log.info(
                "Querying the '{}' table's history for a registration with documentId: {} and version: {}",
                tableName, documentId, version
            );
            String query = String.format(
                "SELECT * FROM history(%s) as h WHERE h.metadata.id = '%s' AND h.metadata.version = %d",
                tableName, documentId, version
            );
            Result result = txn.execute(query);
            List<IonStruct> revisions = toIonStructs(result);
            if (revisions.size() == 0) {
                throw new IllegalStateException(String.format(
                    "Unable to find a historic registration with documentId: %s and version %d",
                    documentId,
                    version
                ));
            }
            IonStruct revisionIon = revisions.get(0);
            log.info("Revision: {}", revisionIon.toPrettyString());
            return QldbRevision.fromIon(revisionIon);
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    /**
     * Redact a historic revision.
     * @param txn
     *              The {@link TransactionExecutor} for lambda execute.
     * @param blockAddress
     *              The {@link BlockAddress} that contains the historic revision.
     * @param tableId
     *              The table ID of the table that contains the document revision.
     * @param documentId
     *              The unique ID of the document revision that will be redacted.
     * @return
     *              The {@link RedactRevisionRequest} containing information about the
     *              redaction request.
     */
    public static RedactRevisionRequest redactRevision(
        final TransactionExecutor txn,
        final BlockAddress blockAddress,
        final String tableId,
        final String documentId
    ) {
        try {
            log.info(
                "Redacting the revision at blockAddress: {} with tableId: {} and documentId: {}",
                blockAddress, tableId, documentId
            );
            final String query = String.format("exec redact_revision ?, '%s', '%s'", tableId, documentId);
            final Result result = txn.execute(query, Constants.MAPPER.writeValueAsIonValue(blockAddress));
            List<IonStruct> results = toIonStructs(result);
            printDocuments(results);
            IonStruct redactRevisionRequestIon = results.stream().findFirst()
                .orElseThrow(() -> new IllegalStateException("Unable to redact the revision."));
            return Constants.MAPPER.readValue(redactRevisionRequestIon, RedactRevisionRequest.class);
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    /**
     * Wait until the {@link QldbRevision} has been redacted. This is because
     * the redaction is an asynchronous process and finishes some time after
     * the redaction is requested.
     * @param revisionSupplier
     *              Supply the {@link QldbRevision} to check whether it has been
     *              redacted. This can be the result of a history query or GetRevision
     *              API call.
     * @return
     *              The {@link QldbRevision} of a document that has been redacted.
     */
    public static QldbRevision waitUntilRedactionCompletes(
        Supplier<QldbRevision> revisionSupplier
    ) throws InterruptedException {
        QldbRevision revision;
        boolean isRedacted;
        do {
            revision = revisionSupplier.get();
            isRedacted = revision.isRedacted();
            if (!isRedacted) {
                log.info("Revision is not yet redacted. Waiting for some time.");
                Thread.sleep(10_000);
            } else {
                log.info("Revision was successfully redacted!");
            }
        } while (!isRedacted);
        return revision;
    }

    public static void main(final String... args) throws InterruptedException {
        final String vin = SampleData.VEHICLES.get(1).getVin();
        final String oldPrimaryOwnerGovId = SampleData.PEOPLE.get(1).getGovId();
        final String newPrimaryOwnerGovId = SampleData.PEOPLE.get(2).getGovId();

        ConnectToLedger.getDriver().execute(txn -> {
            final Person primaryOwner = TransferVehicleOwnership.findPrimaryOwnerForVehicle(txn, vin);
            if (!primaryOwner.getGovId().equals(oldPrimaryOwnerGovId)) {
                // Verify the primary owner.
                throw new IllegalStateException("Incorrect primary owner identified for vehicle, unable to transfer.");
            }

            final String newOwner = Person.getDocumentIdByGovId(txn, newPrimaryOwnerGovId);
            TransferVehicleOwnership.updateVehicleRegistration(txn, vin, newOwner);
        });
        log.info("Successfully transferred vehicle ownership from {} to {}", oldPrimaryOwnerGovId, newPrimaryOwnerGovId);

        GetDigestResult getDigestResult = GetDigest.getDigest(LEDGER_NAME);
        ByteBuffer preRedactionDigest = getDigestResult.getDigest();
        ValueHolder digestTipAddress = getDigestResult.getDigestTipAddress();

        log.info(
            "Let's redact the previous revision of the Vehicle registration with {} as primary owner!",
            oldPrimaryOwnerGovId
        );
        RedactRevisionRequest request = ConnectToLedger.getDriver().execute(txn -> {
            final String tableId = getTableId(txn, VEHICLE_REGISTRATION_TABLE_NAME);
            final String registrationDocumentId = VehicleRegistration.getDocumentIdByVin(txn, vin);
            final String previousOwnerId = Person.getDocumentIdByGovId(txn, oldPrimaryOwnerGovId);
            final BlockAddress blockAddress = historicRegistration(
                txn,
                registrationDocumentId,
                previousOwnerId
            ).getBlockAddress();
            return redactRevision(txn, blockAddress, tableId, registrationDocumentId);
        });
        log.info("Successfully requested a revision redaction: {}", request);

        log.info("Let's wait until redaction completes by querying the table's history for the revision.");
        QldbRevision redactedRevision = waitUntilRedactionCompletes(() ->
            ConnectToLedger.getDriver().execute(txn -> {
                return queryHistoryForRevisionByVersion(
                    txn,
                    VEHICLE_REGISTRATION_TABLE_NAME,
                    request.getDocumentId(),
                    request.getVersion()
                );
            }));
        redactedRevision.verifyRevisionHash();

        log.info("Let's get the revision with proof so we can verify against the previously taken ledger digest!");
        GetRevisionResult getRevisionResult = GetRevision.getRevision(
            LEDGER_NAME,
            redactedRevision.getMetadata().getId(),
            digestTipAddress,
            redactedRevision.getBlockAddress()
        );

        // Verify that the redacted revision's hash can be used to calculate the previously
        // taken digest using the internal hashes in the proof taken after redaction.
        Verifier.verify(
            redactedRevision.getHash(),
            Verifier.convertByteBufferToByteArray(preRedactionDigest),
            getRevisionResult.getProof().getIonText()
        );
        log.info("Successfully verified the redacted revision against a digest taken prior to the redaction!");
    }
}

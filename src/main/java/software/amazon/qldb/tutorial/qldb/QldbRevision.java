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

package software.amazon.qldb.tutorial.qldb;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ionhash.IonHashReader;
import com.amazon.ionhash.IonHashReaderBuilder;
import com.amazon.ionhash.MessageDigestIonHasherProvider;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.ion.IonTimestampSerializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.tutorial.Constants;
import software.amazon.qldb.tutorial.Verifier;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

/**
 * Represents a QldbRevision including both user data and metadata.
 */
public final class QldbRevision {
    private static final Logger log = LoggerFactory.getLogger(QldbRevision.class);
    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    private static MessageDigestIonHasherProvider ionHasherProvider = new MessageDigestIonHasherProvider("SHA-256");
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final BigDecimal ONE_THOUSAND = BigDecimal.valueOf(1000L);

    /**
     * Represents the metadata field of a QLDB Document
     */
    public static class Metadata {
        private final String id;
        private final long version;
        @JsonSerialize(using = IonTimestampSerializers.IonTimestampJavaDateSerializer.class)
        private final Date txTime;
        private final String txId;

        @JsonCreator
        public Metadata(@JsonProperty("id") final String id,
                        @JsonProperty("version") final long version,
                        @JsonProperty("txTime") final Date txTime,
                        @JsonProperty("txId") final String txId) {
            this.id = id;
            this.version = version;
            this.txTime = txTime;
            this.txId = txId;
        }

        /**
         * Gets the unique ID of a QLDB document.
         *
         * @return the document ID.
         */
        public String getId() {
            return id;
        }

        /**
         * Gets the version number of the document in the document's modification history.
         * @return the version number.
         */
        public long getVersion() {
            return version;
        }

        /**
         * Gets the time during which the document was modified.
         *
         * @return the transaction time.
         */
        public Date getTxTime() {
            return txTime;
        }

        /**
         * Gets the transaction ID associated with this document.
         *
         * @return the transaction ID.
         */
        public String getTxId() {
            return txId;
        }

        public static Metadata fromIon(final IonStruct ionStruct) {
            if (ionStruct == null) {
                throw new IllegalArgumentException("Metadata cannot be null");
            }
            try {
                IonString id = (IonString) ionStruct.get("id");
                IonInt version = (IonInt) ionStruct.get("version");
                IonTimestamp txTime = (IonTimestamp) ionStruct.get("txTime");
                IonString txId = (IonString) ionStruct.get("txId");
                if (id == null || version == null || txTime == null || txId == null) {
                    throw new IllegalArgumentException("Document is missing required fields");
                }
                return new Metadata(id.stringValue(), version.longValue(), new Date(txTime.getMillis()), txId.stringValue());
            } catch (ClassCastException e) {
                log.error("Failed to parse ion document");
                throw new IllegalArgumentException("Document members are not of the correct type", e);
            }
        }

        /**
         * Converts a {@link Metadata} object to a string.
         *
         * @return the string representation of the {@link QldbRevision} object.
         */
        @Override
        public String toString() {
            return "Metadata{"
                    + "id='" + id + '\''
                    + ", version=" + version
                    + ", txTime=" + txTime
                    + ", txId='" + txId
                    + '\''
                    + '}';
        }

        /**
         * Check whether two {@link Metadata} objects are equivalent.
         *
         * @return {@code true} if the two objects are equal, {@code false} otherwise.
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) { return true; }
            if (o == null || getClass() != o.getClass()) { return false; }
            Metadata metadata = (Metadata) o;
            return version == metadata.version
                    && id.equals(metadata.id)
                    && txTime.equals(metadata.txTime)
                    && txId.equals(metadata.txId);
        }

        /**
         * Generate a hash code for the {@link Metadata} object.
         *
         * @return the hash code.
         */
        @Override
        public int hashCode() {
            // CHECKSTYLE:OFF - Disabling as we are generating a hashCode of multiple properties.
            return Objects.hash(id, version, txTime, txId);
            // CHECKSTYLE:ON
        }
    }

    private final BlockAddress blockAddress;
    private final Metadata metadata;
    private final byte[] hash;
    private final IonStruct data;

    @JsonCreator
    public QldbRevision(@JsonProperty("blockAddress") final BlockAddress blockAddress,
                        @JsonProperty("metadata") final Metadata metadata,
                        @JsonProperty("hash") final byte[] hash,
                        @JsonProperty("data") final IonStruct data) {
        this.blockAddress = blockAddress;
        this.metadata = metadata;
        this.hash = hash;
        this.data = data;
    }

    /**
     * Gets the unique ID of a QLDB document.
     *
     * @return the {@link BlockAddress} object.
     */
    public BlockAddress getBlockAddress() {
        return blockAddress;
    }

    /**
     * Gets the metadata of the revision.
     *
     * @return the {@link Metadata} object.
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Gets the SHA-256 hash value of the data.
     *
     * @return the byte array representing the hash.
     */
    public byte[] getHash() {
        return hash;
    }

    /**
     * Gets the revision data.
     *
     * @return the revision data.
     */
    public IonStruct getData() {
        return data;
    }

    /**
     * Constructs a new {@link QldbRevision} from an {@link IonStruct}.
     *
     * The specified {@link IonStruct} must include the following fields
     *
     * - blockAddress -- a {@link BlockAddress},
     * - metadata -- a {@link Metadata},
     * - hash -- the document's hash calculated by QLDB,
     * - data -- an {@link IonStruct} containing user data in the document.
     *
     * If any of these fields are missing or are malformed, then throws {@link IllegalArgumentException}.
     *
     * If the document hash calculated from the members of the specified {@link IonStruct} does not match
     * the hash member of the {@link IonStruct} then throws {@link IllegalArgumentException}.
     *
     * @param ionStruct
     *              The {@link IonStruct} that contains a {@link QldbRevision} object.
     * @return the converted {@link QldbRevision} object.
     * @throws IOException if failed to parse parameter {@link IonStruct}.
     */
    public static QldbRevision fromIon(final IonStruct ionStruct) throws IOException {
        try {
            BlockAddress blockAddress = Constants.MAPPER.readValue(ionStruct.get("blockAddress"), BlockAddress.class);
            IonBlob hash = (IonBlob) ionStruct.get("hash");
            IonStruct metadataStruct = (IonStruct) ionStruct.get("metadata");
            IonStruct data = (IonStruct) ionStruct.get("data");
            if (hash == null || data == null) {
                throw new IllegalArgumentException("Document is missing required fields");
            }
            byte[] candidateHash = computeHash(metadataStruct, data);
            if (!Arrays.equals(candidateHash, hash.getBytes())) {
                throw new IllegalArgumentException("Hash entry of QLDB revision and computed hash "
                                                           + "of QLDB revision do not match");
            }
            Metadata metadata = Metadata.fromIon(metadataStruct);
            return new QldbRevision(blockAddress, metadata, hash.getBytes(), data);
        } catch (ClassCastException e) {
            log.error("Failed to parse ion document");
            throw new IllegalArgumentException("Document members are not of the correct type", e);
        }
    }

    /**
     * Calculate the digest of two QLDB hashes.
     *
     * @param metadata
     *              The metadata portion of a document.
     * @param data
     *              The data portion of a document.
     * @return the converted {@link QldbRevision} object.
     */
    public static byte[] computeHash(final IonStruct metadata, final IonStruct data) {
        byte[] metaDataHash = hashIonValue(metadata);
        byte[] dataHash = hashIonValue(data);
        return Verifier.joinHashesPairwise(metaDataHash, dataHash);
    }

    /**
     * Builds a hash value from the given {@link IonValue}.
     *
     * @param ionValue
     *              The {@link IonValue} to hash.
     * @return a byte array representing the hash value.
     */
    private static byte[] hashIonValue(final IonValue ionValue) {
        IonReader reader = SYSTEM.newReader(ionValue);
        IonHashReader hashReader = IonHashReaderBuilder.standard()
                .withHasherProvider(ionHasherProvider)
                .withReader(reader)
                .build();
        while (hashReader.next() != null) {  }
        return hashReader.digest();
    }

    /**
     * Converts a {@link QldbRevision} object to string.
     *
     * @return the string representation of the {@link QldbRevision} object.
     */
    @Override
    public String toString() {
        return "QldbRevision{" +
                "blockAddress=" + blockAddress +
                ", metadata=" + metadata +
                ", hash=" + Arrays.toString(hash) +
                ", data=" + data +
                '}';
    }

    /**
     * Check whether two {@link QldbRevision} objects are equivalent.
     *
     * @return {@code true} if the two objects are equal, {@code false} otherwise.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QldbRevision)) {
            return false;
        }
        final QldbRevision that = (QldbRevision) o;
        return Objects.equals(getBlockAddress(), that.getBlockAddress()) && Objects.equals(getMetadata(),
            that.getMetadata()) && Arrays.equals(getHash(), that.getHash()) && Objects.equals(getData(),
            that.getData());
    }

    /**
     * Create a hash code for the {@link QldbRevision} object.
     *
     * @return the hash code.
     */
    @Override
    public int hashCode() {
        // CHECKSTYLE:OFF - Disabling as we are generating a hashCode of multiple properties.
        int result = Objects.hash(blockAddress, metadata, data);
        // CHECKSTYLE:ON
        result = 31 * result + Arrays.hashCode(hash);
        return result;
    }
}

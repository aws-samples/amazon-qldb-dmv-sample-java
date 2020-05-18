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

package software.amazon.qldb.tutorial.model.streams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import software.amazon.qldb.tutorial.model.DriversLicense;
import software.amazon.qldb.tutorial.model.Person;
import software.amazon.qldb.tutorial.model.Vehicle;
import software.amazon.qldb.tutorial.model.VehicleRegistration;
import software.amazon.qldb.tutorial.qldb.BlockAddress;
import software.amazon.qldb.tutorial.qldb.RevisionMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a Revision including both user data and metadata.
 */
public final class Revision {
    private final BlockAddress blockAddress;
    private final RevisionMetadata metadata;
    private final byte[] hash;
    @JsonDeserialize(using = RevisionDataDeserializer.class)
    private final RevisionData data;

    @JsonCreator
    public Revision(@JsonProperty("blockAddress") final BlockAddress blockAddress,
                    @JsonProperty("metadata") final RevisionMetadata metadata,
                    @JsonProperty("hash") final byte[] hash,
                    @JsonProperty("data") final RevisionData data) {
        this.blockAddress = blockAddress;
        this.metadata = metadata;
        this.hash = hash;
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Revision revision = (Revision) o;

        if (!Objects.equals(blockAddress, revision.blockAddress)) {
            return false;
        }
        if (!Objects.equals(metadata, revision.metadata)) {
            return false;
        }
        if (!Arrays.equals(hash, revision.hash)) {
            return false;
        }
        return Objects.equals(data, revision.data);
    }

    @Override
    public int hashCode() {
        int result = blockAddress != null ? blockAddress.hashCode() : 0;
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(hash);
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }

    /**
     * Converts a {@link Revision} object to string.
     *
     * @return the string representation of the {@link Revision} object.
     */
    @Override
    public String toString() {
        return "Revision{" +
                "blockAddress=" + blockAddress +
                ", metadata=" + metadata +
                ", hash=" + Arrays.toString(hash) +
                ", data=" + data +
                '}';
    }

    public BlockAddress getBlockAddress() {
        return blockAddress;
    }

    public RevisionMetadata getMetadata() {
        return metadata;
    }

    public byte[] getHash() {
        return hash;
    }

    public RevisionData getData() {
        return data;
    }

    public static class RevisionDataDeserializer extends JsonDeserializer<RevisionData> {

        @Override
        public RevisionData deserialize(JsonParser jp, DeserializationContext dc) throws IOException {
            TableInfo tableInfo = (TableInfo) jp.getParsingContext().getParent().getCurrentValue();
            RevisionData revisionData;
            switch (tableInfo.getTableName()) {
                case "VehicleRegistration":
                    revisionData = jp.readValueAs(VehicleRegistration.class);
                    break;
                case "Person":
                    revisionData = jp.readValueAs(Person.class);
                    break;
                case "DriversLicense":
                    revisionData = jp.readValueAs(DriversLicense.class);
                    break;
                case "Vehicle":
                    revisionData = jp.readValueAs(Vehicle.class);
                    break;
                default:
                    throw new RuntimeException("Unsupported table " + tableInfo.getTableName());
            }

            return revisionData;
        }
    }
}

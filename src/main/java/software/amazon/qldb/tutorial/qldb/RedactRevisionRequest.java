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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Contains information about an individual revision that was redacted
 * as part of a transaction.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RedactRevisionRequest {
    private final BlockAddress blockAddress;
    private final String tableId;
    private final String documentId;
    private final long version;

    @JsonCreator
    public RedactRevisionRequest(@JsonProperty("blockAddress") final BlockAddress blockAddress,
                                 @JsonProperty("tableId") final String tableId,
                                 @JsonProperty("documentId") final String documentId,
                                 @JsonProperty("version") final long version) {
        this.blockAddress = blockAddress;
        this.tableId = tableId;
        this.documentId = documentId;
        this.version = version;
    }

    public BlockAddress getBlockAddress() {
        return blockAddress;
    }

    public String getTableId() {
        return tableId;
    }

    public String getDocumentId() {
        return documentId;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RedactRevisionRequest)) {
            return false;
        }

        final RedactRevisionRequest that = (RedactRevisionRequest) o;

        if (getBlockAddress() != null ? !getBlockAddress().equals(that.getBlockAddress()) : that.getBlockAddress() != null) {
            return false;
        }

        if (getTableId() != null ? !getTableId().equals(that.getTableId()) : that.getTableId() != null) {
            return false;
        }

        if (getVersion() != that.getVersion()) {
            return false;
        }
        return getDocumentId() != null ? getDocumentId().equals(that.getDocumentId()) : that.getDocumentId() == null;
    }

    @Override
    public int hashCode() {
        int result = getBlockAddress() != null ? getBlockAddress().hashCode() : 0;
        result = 31 * result + (getTableId() != null ? getTableId().hashCode() : 0);
        result = 31 * result + (getDocumentId() != null ? getDocumentId().hashCode() : 0);
        result = 31 * result + Long.hashCode(getVersion());
        return result;
    }

    @Override
    public String toString() {
        return "RedactRevision{"
            + "blockAddress=" + blockAddress
            + ", tableId='" + tableId + "'"
            + ", documentId='" + documentId + "'"
            + ", version=" + version
            + "}";
    }
}

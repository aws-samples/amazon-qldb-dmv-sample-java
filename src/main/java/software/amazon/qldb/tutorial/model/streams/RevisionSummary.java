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

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents the revision summary that appears in the {@link
 * BlockSummaryRecord}. Some revisions might not have a documentId. These are
 * internal-only system revisions that don't contain user data. Only the
 * revisions that do have a document ID are published in separate revision
 * details record.
 */
public final class RevisionSummary {

    private String documentId;
    private byte[] hash;

    @JsonCreator
    public RevisionSummary(@JsonProperty("documentId") String documentId, @JsonProperty("hash") byte[] hash) {
        this.documentId = documentId;
        this.hash = hash;
    }

    @Override
    public String toString() {
        return "RevisionSummary{" +
                "documentId='" + documentId + '\'' +
                ", hash=" + Arrays.toString(hash) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RevisionSummary that = (RevisionSummary) o;

        if (!Objects.equals(documentId, that.documentId)) {
            return false;
        }
        return Arrays.equals(hash, that.hash);
    }

    @Override
    public int hashCode() {
        int result = documentId != null ? documentId.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(hash);
        return result;
    }

    public String getDocumentId() {
        return documentId;
    }

    public byte[] getHash() {
        return hash;
    }
}

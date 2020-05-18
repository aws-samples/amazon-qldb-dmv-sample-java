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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Represents a revision record on the QLDB stream. A revision details record
 * represents a document revision that is committed to your ledger. The payload
 * contains all of the attributes from the committed view of the revision, along
 * with the associated table name and table ID.
 */
public final class RevisionDetailsRecord implements StreamRecord.StreamRecordPayload {
    private TableInfo tableInfo;
    private Revision revision;

    public RevisionDetailsRecord(@JsonProperty("tableInfo") TableInfo tableInfo, @JsonProperty("revision") Revision revision) {
        this.tableInfo = tableInfo;
        this.revision = revision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RevisionDetailsRecord that = (RevisionDetailsRecord) o;

        if (!Objects.equals(tableInfo, that.tableInfo)) {
            return false;
        }
        return Objects.equals(revision, that.revision);
    }

    @Override
    public int hashCode() {
        int result = tableInfo != null ? tableInfo.hashCode() : 0;
        result = 31 * result + (revision != null ? revision.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RevisionDetailsRecord{" +
                "tableInfo=" + tableInfo +
                ", revision=" + revision +
                '}';
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public Revision getRevision() {
        return revision;
    }
}

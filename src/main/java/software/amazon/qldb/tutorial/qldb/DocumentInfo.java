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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Information about an individual document in a table and the transactions associated with that document
 * in the given JournalBlock.
 */
public class DocumentInfo {

    private String tableName;
    private String tableId;
    private List<Integer> statementIndexList;

    @JsonCreator
    public DocumentInfo(@JsonProperty("tableName") final String tableName,
                        @JsonProperty("tableId") final String tableId,
                        @JsonProperty("statements") final List<Integer> statementIndexList) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.statementIndexList = statementIndexList;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableId() {
        return tableId;
    }

    @JsonProperty("statements")
    public List<Integer> getStatementIndexList() {
        return statementIndexList;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentInfo)) {
            return false;
        }

        final DocumentInfo that = (DocumentInfo) o;

        if (!getTableName().equals(that.getTableName())) {
            return false;
        }
        if (!getTableId().equals(that.getTableId())) {
            return false;
        }
        return getStatementIndexList().equals(that.getStatementIndexList());
    }

    @Override
    public int hashCode() {
        int result = getTableName().hashCode();
        result = 31 * result + getTableId().hashCode();
        result = 31 * result + getStatementIndexList().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DocumentInfo{"
            + "tableName='" + tableName + '\''
            + ", tableId='" + tableId + '\''
            + ", statementIndexList=" + statementIndexList + '}';
    }
}

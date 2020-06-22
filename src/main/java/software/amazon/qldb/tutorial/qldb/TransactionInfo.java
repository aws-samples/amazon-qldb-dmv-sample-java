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
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Information about the transaction. Contains all the statements executed as
 * part of the transaction and mapping between the documents to
 * tableName/tableId which were updated as part of the transaction.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransactionInfo {

    private List<StatementInfo> statements;
    private Map<String, DocumentInfo> documents;

    @JsonCreator
    public TransactionInfo(@JsonProperty("statements") final List<StatementInfo> statements,
                           @JsonProperty("documents") final Map<String, DocumentInfo> documents) {
        this.statements = statements;
        this.documents = documents;
    }

    public List<StatementInfo> getStatements() {
        return statements;
    }

    public Map<String, DocumentInfo> getDocuments() {
        return documents;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TransactionInfo)) {
            return false;
        }

        final TransactionInfo that = (TransactionInfo) o;

        if (getStatements() != null ? !getStatements().equals(that.getStatements()) : that.getStatements() != null) {
            return false;
        }
        return getDocuments() != null ? getDocuments().equals(that.getDocuments()) : that.getDocuments() == null;
    }

    @Override
    public int hashCode() {
        int result = getStatements() != null ? getStatements().hashCode() : 0;
        result = 31 * result + (getDocuments() != null ? getDocuments().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TransactionInfo{"
            + "statements=" + statements
            + ", documents=" + documents
            + '}';
    }
}

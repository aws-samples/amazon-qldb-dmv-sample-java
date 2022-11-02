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
import java.util.List;

/**
 * Information about redactions in a transaction. Contains all the
 * revisions that were redacted as a result of this transaction.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RedactionInfo {

    private final List<RedactRevisionRequest> revisions;

    @JsonCreator
    public RedactionInfo(@JsonProperty("revisions") final List<RedactRevisionRequest> revisions) {
        this.revisions = revisions;
    }

    public List<RedactRevisionRequest> getRevisions() {
        return revisions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RedactionInfo)) {
            return false;
        }

        final RedactionInfo that = (RedactionInfo) o;

        return getRevisions() != null ? getRevisions().equals(that.getRevisions()) : that.getRevisions() == null;
    }

    @Override
    public int hashCode() {
        return getRevisions() != null ? getRevisions().hashCode() : 0;
    }

    @Override
    public String toString() {
        return "RedactionInfo{"
            + "revisions=" + revisions
            + "}";
    }
}

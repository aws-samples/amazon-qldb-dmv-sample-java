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

package software.amazon.qldb.tutorial.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Contains info about a given index.
 */
public class IndexInfo {
    private String identifier;
    private String expr;
    private String status;

    @JsonCreator
    public IndexInfo(@JsonProperty("identifier") final String identifier,
                     @JsonProperty("expr") final String expr,
                     @JsonProperty("status") final String status) {
        this.identifier = identifier;
        this.expr = expr;
        this.status = status;
    }

    /**
     * An index's id. Upon creation, QLDB assigns a unique id to an index.
     * @return The QLDB assigned index id.
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * The indexed document path. A string in the form, {@code "[<fieldName>]"}.
     * @return The indexed document path representation.
     */
    public String getExpr() {
        return expr;
    }

    /**
     * The status of the index. One of {BUILDING, FINALIZING, ONLINE, FAILED}.
     * The index is not used by QLDB in queries until the status is ONLINE.
     * @return The index status.
     */
    public String getStatus() {
        return status;
    }
}

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

import java.util.Arrays;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.ion.IonTimestampSerializers;

/**
 * Contains information about an individual statement run as part of a transaction.
 */
public class StatementInfo {

    private String statement;
    @JsonSerialize(using = IonTimestampSerializers.IonTimestampJavaDateSerializer.class)
    private Date startTime;
    private byte[] statementDigest;

    @JsonCreator
    public StatementInfo(@JsonProperty("statement") final String statement,
                         @JsonProperty("startTime") final Date startTime,
                         @JsonProperty("statementDigest") final byte[] statementDigest) {
        this.statement = statement;
        this.startTime = startTime;
        this.statementDigest = statementDigest;
    }

    public String getStatement() {
        return statement;
    }

    public Date getStartTime() {
        return startTime;
    }

    public byte[] getStatementDigest() {
        return statementDigest;
    }

    @Override
    public String toString() {
        return "StatementInfo{"
            + "statement='" + statement + '\''
            + ", startTime=" + startTime
            + ", statementDigest=" + Arrays.toString(statementDigest)
            + '}';
    }
}

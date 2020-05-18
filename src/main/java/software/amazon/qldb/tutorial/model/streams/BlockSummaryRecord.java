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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.ion.IonTimestampSerializers;
import software.amazon.qldb.tutorial.qldb.BlockAddress;
import software.amazon.qldb.tutorial.qldb.TransactionInfo;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a summary for a Journal Block that was recorded after executing a
 * transaction in the ledger.
 */
public final class BlockSummaryRecord implements StreamRecord.StreamRecordPayload {
    private BlockAddress blockAddress;
    private String transactionId;
    @JsonSerialize(using = IonTimestampSerializers.IonTimestampJavaDateSerializer.class)
    private Date blockTimestamp;
    private byte[] blockHash;
    private byte[] entriesHash;
    private byte[] previousBlockHash;
    private byte[][] entriesHashList;
    private TransactionInfo transactionInfo;
    private List<RevisionSummary> revisionSummaries;

    @JsonCreator
    public BlockSummaryRecord(@JsonProperty("blockAddress") final BlockAddress blockAddress,
                              @JsonProperty("transactionId") final String transactionId,
                              @JsonProperty("blockTimestamp") final Date blockTimestamp,
                              @JsonProperty("blockHash") final byte[] blockHash,
                              @JsonProperty("entriesHash") final byte[] entriesHash,
                              @JsonProperty("previousBlockHash") final byte[] previousBlockHash,
                              @JsonProperty("entriesHashList") final byte[][] entriesHashList,
                              @JsonProperty("transactionInfo") final TransactionInfo transactionInfo,
                              @JsonProperty("revisionSummaries") final List<RevisionSummary> revisionSummaries) {
        this.blockAddress = blockAddress;
        this.transactionId = transactionId;
        this.blockTimestamp = blockTimestamp;
        this.blockHash = blockHash;
        this.entriesHash = entriesHash;
        this.previousBlockHash = previousBlockHash;
        this.entriesHashList = entriesHashList;
        this.transactionInfo = transactionInfo;
        this.revisionSummaries = revisionSummaries;
    }

    @Override
    public String toString() {
        return "JournalBlock{"
                + "blockAddress=" + blockAddress
                + ", transactionId='" + transactionId + '\''
                + ", blockTimestamp=" + blockTimestamp
                + ", blockHash=" + Arrays.toString(blockHash)
                + ", entriesHash=" + Arrays.toString(entriesHash)
                + ", previousBlockHash=" + Arrays.toString(previousBlockHash)
                + ", entriesHashList=" + Arrays.stream(entriesHashList).map(Arrays::toString).collect(Collectors.toList())
                + ", transactionInfo=" + transactionInfo
                + ", revisionSummaries=" + revisionSummaries
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockSummaryRecord that = (BlockSummaryRecord) o;

        if (!Objects.equals(blockAddress, that.blockAddress)) {
            return false;
        }
        if (!Objects.equals(transactionId, that.transactionId)) {
            return false;
        }
        if (!Objects.equals(blockTimestamp, that.blockTimestamp)) {
            return false;
        }
        if (!Arrays.equals(blockHash, that.blockHash)) {
            return false;
        }
        if (!Arrays.equals(entriesHash, that.entriesHash)) {
            return false;
        }
        if (!Arrays.equals(previousBlockHash, that.previousBlockHash)) {
            return false;
        }
        if (!Arrays.deepEquals(entriesHashList, that.entriesHashList)) {
            return false;
        }
        if (!Objects.equals(transactionInfo, that.transactionInfo)) {
            return false;
        }
        return Objects.equals(revisionSummaries, that.revisionSummaries);
    }

    @Override
    public int hashCode() {
        int result = blockAddress != null ? blockAddress.hashCode() : 0;
        result = 31 * result + (transactionId != null ? transactionId.hashCode() : 0);
        result = 31 * result + (blockTimestamp != null ? blockTimestamp.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(blockHash);
        result = 31 * result + Arrays.hashCode(entriesHash);
        result = 31 * result + Arrays.hashCode(previousBlockHash);
        result = 31 * result + Arrays.deepHashCode(entriesHashList);
        result = 31 * result + (transactionInfo != null ? transactionInfo.hashCode() : 0);
        result = 31 * result + (revisionSummaries != null ? revisionSummaries.hashCode() : 0);
        return result;
    }

    public BlockAddress getBlockAddress() {
        return blockAddress;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Date getBlockTimestamp() {
        return blockTimestamp;
    }

    public byte[] getBlockHash() {
        return blockHash;
    }

    public byte[] getEntriesHash() {
        return entriesHash;
    }

    public byte[] getPreviousBlockHash() {
        return previousBlockHash;
    }

    public byte[][] getEntriesHashList() {
        return entriesHashList;
    }

    public TransactionInfo getTransactionInfo() {
        return transactionInfo;
    }

    public List<RevisionSummary> getRevisionSummaries() {
        return revisionSummaries;
    }

}

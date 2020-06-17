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

package software.amazon.qldb.tutorial;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonStruct;
import com.amazonaws.services.qldb.AmazonQLDB;
import com.amazonaws.services.qldb.model.GetBlockRequest;
import com.amazonaws.services.qldb.model.GetBlockResult;
import com.amazonaws.services.qldb.model.GetDigestResult;
import com.amazonaws.services.qldb.model.ValueHolder;

import software.amazon.qldb.tutorial.qldb.BlockAddress;
import software.amazon.qldb.tutorial.qldb.JournalBlock;
import software.amazon.qldb.tutorial.qldb.QldbRevision;
import software.amazon.qldb.tutorial.model.SampleData;
import software.amazon.qldb.tutorial.qldb.QldbStringUtils;

/**
 * Get a journal block from a QLDB ledger.
 *
 * After getting the block, we get the digest of the ledger and validate the
 * proof returned in the getBlock response.
 *
 * <p>
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public class GetBlock {

    public static final Logger log = LoggerFactory.getLogger(QueryHistory.class);
    public static AmazonQLDB client = CreateLedger.getClient();

    private GetBlock() {}

    public static void main(String[] args) {
        try {
            List<IonStruct> results = ConnectToLedger.getDriver().execute(txn -> {
                final String vin = SampleData.VEHICLES.get(1).getVin();
                return GetRevision.queryRegistrationsByVin(txn, vin);
            });
            BlockAddress blockAddress = Constants.MAPPER.readValue(results.get(0), QldbRevision.class).getBlockAddress();
            verifyBlock(Constants.LEDGER_NAME, blockAddress);
        } catch (Exception e) {
            log.error("Unable to query vehicle registration by Vin.", e);
        }
    }

    public static GetBlockResult getBlock(String ledgerName, BlockAddress blockAddress) {
        log.info("Let's get the block for block address {} of the ledger named {}.", blockAddress, ledgerName);
        try {
            GetBlockRequest request = new GetBlockRequest()
                .withName(ledgerName)
                .withBlockAddress(new ValueHolder().withIonText(Constants.MAPPER.writeValueAsIonValue(blockAddress).toString()));
            GetBlockResult result = client.getBlock(request);
            log.info("Success. GetBlock: {}.", QldbStringUtils.toUnredactedString(result));
            return result;
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    public static GetBlockResult getBlockWithProof(String ledgerName, BlockAddress blockAddress, BlockAddress tipBlockAddress) {
        log.info("Let's get the block for block address {}, digest tip address {}, for the ledger named {}.", blockAddress,
                 tipBlockAddress, ledgerName);
        try {
            GetBlockRequest request = new GetBlockRequest()
                .withName(ledgerName)
                .withBlockAddress(new ValueHolder().withIonText(Constants.MAPPER.writeValueAsIonValue(blockAddress).toString()))
                .withDigestTipAddress(new ValueHolder().withIonText(Constants.MAPPER.writeValueAsIonValue(tipBlockAddress)
                                                                            .toString()));
            GetBlockResult result = client.getBlock(request);
            log.info("Success. GetBlock: {}.", QldbStringUtils.toUnredactedString(result));
            return result;
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    public static void verifyBlock(String ledgerName, BlockAddress blockAddress) throws Exception {
        log.info("Lets verify blocks for ledger with name={}.", ledgerName);

        try {
            log.info("First, let's get a digest");
            GetDigestResult digestResult = GetDigest.getDigest(ledgerName);
            BlockAddress tipBlockAddress = Constants.MAPPER.readValue(digestResult.getDigestTipAddress().getIonText(),
                                                                      BlockAddress.class);

            ValueHolder digestTipAddress = digestResult.getDigestTipAddress();
            byte[] digestBytes = Verifier.convertByteBufferToByteArray(digestResult.getDigest());

            log.info("Got a ledger digest. Digest end address={}, digest={}.",
                QldbStringUtils.toUnredactedString(digestTipAddress),
                Verifier.toBase64(digestBytes));

            GetBlockResult getBlockResult = getBlockWithProof(ledgerName, blockAddress, tipBlockAddress);
            JournalBlock block = Constants.MAPPER.readValue(getBlockResult.getBlock().getIonText(), JournalBlock.class);

            boolean verified = Verifier.verify(
                block.getBlockHash(),
                digestBytes,
                getBlockResult.getProof().getIonText()
            );

            if (!verified) {
                throw new AssertionError("Block is not verified!");
            } else {
                log.info("Success! The block is verified.");
            }

            byte[] alteredDigest = Verifier.flipRandomBit(digestBytes);
            log.info("Let's try flipping one bit in the digest and assert that the block is NOT verified. "
                + "The altered digest is: {}.", Verifier.toBase64(alteredDigest));
            verified = Verifier.verify(
                block.getBlockHash(),
                alteredDigest,
                getBlockResult.getProof().getIonText()
            );

            if (verified) {
                throw new AssertionError("Expected block to not be verified against altered digest.");
            } else {
                log.info("Success! As expected flipping a bit in the digest causes verification to fail.");
            }

            byte[] alteredBlockHash = Verifier.flipRandomBit(block.getBlockHash());
            log.info("Let's try flipping one bit in the block's hash and assert that the block is NOT "
                + "verified. The altered block hash is: {}.", Verifier.toBase64(alteredBlockHash));
            verified = Verifier.verify(
                alteredBlockHash,
                digestBytes,
                getBlockResult.getProof().getIonText()
            );

            if (verified) {
                throw new AssertionError("Expected altered block hash to not be verified against digest.");
            } else {
                log.info("Success! As expected flipping a bit in the block hash causes verification to fail.");
            }

        } catch (Exception e) {
            log.error("Failed to verify blocks in the ledger with name={}.", ledgerName, e);
            throw e;
        }
    }
}

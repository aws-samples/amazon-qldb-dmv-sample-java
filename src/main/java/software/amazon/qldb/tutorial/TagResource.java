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

import com.amazonaws.services.qldb.AmazonQLDB;
import com.amazonaws.services.qldb.model.CreateLedgerRequest;
import com.amazonaws.services.qldb.model.CreateLedgerResult;
import com.amazonaws.services.qldb.model.ListTagsForResourceRequest;
import com.amazonaws.services.qldb.model.ListTagsForResourceResult;
import com.amazonaws.services.qldb.model.PermissionsMode;
import com.amazonaws.services.qldb.model.TagResourceRequest;
import com.amazonaws.services.qldb.model.UntagResourceRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tagging and un-tagging resources, including tag on create.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public final class TagResource {
    public static final Logger log = LoggerFactory.getLogger(TagResource.class);
    public static final String LEDGER_NAME = Constants.LEDGER_NAME_WITH_TAGS;
    public static AmazonQLDB client = CreateLedger.getClient();
    public static Map<String, String> CREATE_TAGS;
    public static Map<String, String> ADD_TAGS;
    public static List<String> REMOVE_TAGS;

    private TagResource() { }

    static {
        CREATE_TAGS = new HashMap<>();
        CREATE_TAGS.put("IsTest", "true");
        CREATE_TAGS.put("Domain", "Test");

        REMOVE_TAGS = new ArrayList<>();
        REMOVE_TAGS.add("IsTest");

        ADD_TAGS = new HashMap<>();
        ADD_TAGS.put("Domain", "Prod");
    }

    /**
     * Create a ledger with the given tags.
     *
     * @param ledgerName
     *              Name of the ledger to be created.
     * @param tags
     *              The map of key-value pairs to create the ledger with.
     * @return {@link CreateLedgerResult}.
     */
    public static CreateLedgerResult createWithTags(final String ledgerName, final Map<String, String> tags) {
        log.info("Let's create the ledger with name: {}...", ledgerName);
        CreateLedgerRequest request = new CreateLedgerRequest()
                .withName(ledgerName)
                .withTags(tags)
                .withPermissionsMode(PermissionsMode.ALLOW_ALL);
        CreateLedgerResult result = client.createLedger(request);
        log.info("Success. Ledger state: {}", result.getState());
        return result;
    }

    /**
     * Add tags to a resource.
     *
     * @param resourceArn
     *              The Amazon Resource Name (ARN) of the ledger to which to add the tags.
     * @param tags
     *              The map of key-value pairs to add to a ledger.
     */
    public static void tagResource(final String resourceArn, final Map<String, String> tags) {
        log.info("Let's add tags {} for resource with arn: {}...", tags, resourceArn);
        TagResourceRequest request = new TagResourceRequest()
                .withResourceArn(resourceArn)
                .withTags(tags);
        client.tagResource(request);
        log.info("Successfully added tags.");
    }

    /**
     * Remove one or more tags from the specified QLDB resource.
     *
     * @param resourceArn
     *              The Amazon Resource Name (ARN) of the ledger from which to remove the tags.
     * @param tagKeys
     *              The list of tag keys to remove.
     */
    public static void untagResource(final String resourceArn, final List<String> tagKeys) {
        log.info("Let's remove tags {} for resource with arn: {}...", tagKeys, resourceArn);
        UntagResourceRequest request = new UntagResourceRequest()
                .withResourceArn(resourceArn)
                .withTagKeys(tagKeys);
        client.untagResource(request);
        log.info("Successfully removed tags.");
    }

    /**
     * Returns all tags for a specified Amazon QLDB resource.
     *
     * @param resourceArn
     *              The Amazon Resource Name (ARN) for which to list tags off.
     * @return {@link ListTagsForResourceResult}.
     */
    public static ListTagsForResourceResult listTags(final String resourceArn) {
        log.info("Let's list the tags for resource with arn: {}...", resourceArn);
        ListTagsForResourceRequest request = new ListTagsForResourceRequest()
                .withResourceArn(resourceArn);
        ListTagsForResourceResult result = client.listTagsForResource(request);
        log.info("Success. Tags: {}", result.getTags());
        return result;
    }

    public static void main(final String... args) throws Exception {
        try {
            String resourceArn = createWithTags(LEDGER_NAME, CREATE_TAGS).getArn();

            CreateLedger.waitForActive(LEDGER_NAME);

            listTags(resourceArn);

            untagResource(resourceArn, REMOVE_TAGS);

            listTags(resourceArn);

            tagResource(resourceArn, ADD_TAGS);

            listTags(resourceArn);

        } catch (Exception e) {
            log.error("Unable to tag resources!", e);
            throw e;
        }
    }
}

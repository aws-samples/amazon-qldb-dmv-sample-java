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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.AttachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyResult;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.qldb.AmazonQLDB;
import com.amazonaws.services.qldb.AmazonQLDBClientBuilder;
import com.amazonaws.services.qldb.model.DescribeJournalS3ExportResult;
import com.amazonaws.services.qldb.model.ExportJournalToS3Request;
import com.amazonaws.services.qldb.model.ExportJournalToS3Result;
import com.amazonaws.services.qldb.model.ExportStatus;
import com.amazonaws.services.qldb.model.InvalidParameterException;
import com.amazonaws.services.qldb.model.S3EncryptionConfiguration;
import com.amazonaws.services.qldb.model.S3ExportConfiguration;
import com.amazonaws.services.qldb.model.S3ObjectEncryptionType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Export a journal to S3.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 *
 * This code requires an S3 bucket. You can provide the name of an S3 bucket that
 * you wish to use via the arguments (args[0]). The code will check if the bucket
 * exists and create it if not. If you don't provide a bucket name, the code will
 * create a unique bucket for the purposes of this tutorial.
 *
 * Optionally, you can provide an IAM role ARN to use for the journal export via
 * the arguments (args[1]). Otherwise, the code will create and use a role named
 * "QLDBTutorialJournalExportRole".
 *
 * S3 Export Encryption:
 * Optionally, you can provide a KMS key ARN to use for S3-KMS encryption, via
 * the arguments (args[2]). The tutorial code will fail if you provide a KMS key
 * ARN that doesn't exist.
 *
 * If KMS Key ARN is not provided, the Tutorial Code will use
 * SSE-S3 for the S3 Export.
 *
 * If provided, the target KMS Key is expected to have at least the following
 * KeyPolicy:
 * -------------
 * CustomCmkForQLDBExportEncryption:
 *     Type: AWS::KMS::Key
 *     Properties:
 *       KeyUsage: ENCRYPT_DECRYPT
 *       KeyPolicy:
 *         Version: "2012-10-17"
 *         Id: key-default-1
 *         Statement:
 *         - Sid: Grant Permissions for QLDB to use the key
 *           Effect: Allow
 *           Principal:
 *             Service: us-east-1.qldb.amazonaws.com
 *           Action:
 *             - kms:Encrypt
 *             - kms:GenerateDataKey
 *           # In a key policy, you use "*" for the resource, which means "this CMK."
 *           # A key policy applies only to the CMK it is attached to.
 *           Resource: '*'
 * -------------
 * Please see the KMS key policy developer guide here:
 * https://docs.aws.amazon.com/kms/latest/developerguide/key-policies.html
 */
public final class ExportJournal {
    public static final Logger log = LoggerFactory.getLogger(ExportJournal.class);

    public static AmazonQLDB client = getClient();

    static final Long DEFAULT_EXPORT_TIMEOUT_MS = Duration.ofMinutes(10).toMillis();

    private static final String POLICY_TEMPLATE =
                    "{" +
                    "   \"Version\" : \"2012-10-17\"," +
                    "   \"Statement\": [ {statements} ]" +
                    "}";

    public static String ASSUME_ROLE_POLICY =
            POLICY_TEMPLATE.replace("{statements}",
                    "   {" +
                            "       \"Effect\": \"Allow\"," +
                            "       \"Principal\": {" +
                            "           \"Service\": [\"qldb.amazonaws.com\"]" +
                            "       }," +
                            "   \"Action\": [ \"sts:AssumeRole\" ]" +
                            "   }");

    private static final String EXPORT_ROLE_S3_STATEMENT_TEMPLATE =
                    "{" +
                    "   \"Sid\": \"QLDBJournalExportS3Permission\"," +
                    "   \"Action\": [\"s3:PutObject\", \"s3:PutObjectAcl\"]," +
                    "   \"Effect\": \"Allow\"," +
                    "   \"Resource\": \"arn:aws:s3:::{bucket_name}/*\"" +
                    "}";

    private static final String EXPORT_ROLE_KMS_STATEMENT_TEMPLATE =
                    "{" +
                    "   \"Sid\": \"QLDBJournalExportKMSPermission\"," +
                    "   \"Action\": [\"kms:GenerateDataKey\"]," +
                    "   \"Effect\": \"Allow\"," +
                    "   \"Resource\": \"{kms_arn}\"" +
                    "}";

    private static final String EXPORT_ROLE_NAME = "QLDBTutorialJournalExportRole";
    private static final String EXPORT_ROLE_POLICY_NAME = "QLDBTutorialJournalExportRolePolicy";

    private static final Long EXPORT_COMPLETION_POLL_PERIOD_MS = 10_000L;
    private static final Long JOURNAL_EXPORT_TIME_WINDOW_MINUTES = 10L;

    private ExportJournal() { }

    public static AmazonQLDB getClient() {
        return AmazonQLDBClientBuilder.standard().build();
    }

    public static void main(final String... args) throws Exception {
        String s3BucketName;
        String kmsArn = null;
        String roleArn = null;

        if (args.length >= 1) {
            s3BucketName = args[0];
            if (args.length >= 2) {
                roleArn = args[1];
            }
            // KMS Key ARN is an optional argument.
            // If not provided, SSE-S3 is used for exporting to S3 bucket.
            if (args.length == 3) {
                kmsArn = args[2];
            }
        } else {
            String accountId = AWSSecurityTokenServiceClientBuilder.defaultClient()
                .getCallerIdentity(new GetCallerIdentityRequest()).getAccount();
            s3BucketName = Constants.JOURNAL_EXPORT_S3_BUCKET_NAME_PREFIX + "-" + accountId;
        }

        S3EncryptionConfiguration s3EncryptionConfiguration;
        if (kmsArn == null) {
            s3EncryptionConfiguration =
                    new S3EncryptionConfiguration().withObjectEncryptionType(S3ObjectEncryptionType.SSE_S3);
        } else {
            s3EncryptionConfiguration =
                    new S3EncryptionConfiguration().withObjectEncryptionType(S3ObjectEncryptionType.SSE_KMS)
                            .withKmsKeyArn(kmsArn);
        }
        createJournalExportAndAwaitCompletion(Constants.LEDGER_NAME, s3BucketName, Constants.LEDGER_NAME + "/",
                roleArn, s3EncryptionConfiguration, DEFAULT_EXPORT_TIMEOUT_MS);
    }

    /**
     * Create a new journal export on a S3 bucket and wait for its completion.
     *
     * @param ledgerName
     *              The name of the bucket to be created.
     * @param s3BucketName
     *              The name of the S3 bucket to create journal export on.
     * @param s3Prefix
     *              The optional prefix name for the output objects of the export.
     * @param roleArn
     *              The IAM role ARN to be used when exporting the journal.
     * @param encryptionConfiguration
     *              The encryption settings to be used by the export job to write data in the given S3 bucket.
     * @param awaitTimeoutMs
     *              Milliseconds to wait for export to complete.
     * @return {@link ExportJournalToS3Result} from QLDB.
     * @throws InterruptedException if thread is being interrupted while waiting for the export to complete.
     */
    public static ExportJournalToS3Result createJournalExportAndAwaitCompletion(
            String ledgerName, String s3BucketName,
            String s3Prefix, String roleArn,
            S3EncryptionConfiguration encryptionConfiguration,
            long awaitTimeoutMs) throws InterruptedException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        createS3BucketIfNotExists(s3BucketName, s3Client);
        if (roleArn == null) {
            roleArn = createExportRole(EXPORT_ROLE_NAME, AmazonIdentityManagementClientBuilder.defaultClient(),
                    s3BucketName, encryptionConfiguration.getKmsKeyArn(), EXPORT_ROLE_POLICY_NAME);
        }

        try {
            Date startTime = Date.from(Instant.now().minus(JOURNAL_EXPORT_TIME_WINDOW_MINUTES, ChronoUnit.MINUTES));
            Date endTime = Date.from(Instant.now());

            ExportJournalToS3Result exportJournalToS3Result = createExport(ledgerName, startTime, endTime, s3BucketName,
                    s3Prefix, encryptionConfiguration, roleArn);

            // Wait for export to complete.
            waitForExportToComplete(Constants.LEDGER_NAME, exportJournalToS3Result.getExportId(), awaitTimeoutMs);
            log.info("JournalS3Export for exportId " + exportJournalToS3Result.getExportId() + " is completed.");
            return exportJournalToS3Result;
        } catch (Exception e) {
            log.error("Unable to create an export!", e);
            throw e;
        }
    }

    /**
     * If the bucket passed does not exist, the bucket will be created.
     *
     * @param s3BucketName
     *              The name of the bucket to be created.
     * @param s3Client
     *              The low-level S3 client.
     */
    public static void createS3BucketIfNotExists(String s3BucketName, AmazonS3 s3Client) {
        if (!s3Client.doesBucketExistV2(s3BucketName)) {
            log.info("S3 bucket " + s3BucketName + " does not exist. Creating it now.");
            try {
                s3Client.createBucket(s3BucketName);
                log.info("Bucket with name " + s3BucketName + " created.");
            } catch (Exception e) {
                log.error("Unable to create S3 bucket named " + s3BucketName);
                throw e;
            }
        }
    }

    /**
     * Create a new export rule and a new managed policy for the current AWS account.
     *
     * @param roleName
     *              Name of the role to be created.
     * @param iamClient
     *              A low-level service client.
     * @param s3Bucket
     *              If {@code kmsArn} is {@code null}, create a new ARN using the given bucket name.
     * @param kmsArn
     *              Optional KMS Key ARN used to configure the {@code rolePolicyStatement}.
     * @param rolePolicyName
     *              Name for the role policy to be created.
     * @return the newly created {@code roleArn}.
     */
    public static String createExportRole(String roleName, AmazonIdentityManagement iamClient,
                                          String s3Bucket, String kmsArn, String rolePolicyName) {

        GetRoleRequest getRoleRequest = new GetRoleRequest().withRoleName(roleName);
        try {
            log.info("Trying to retrieve role with name: " + roleName);
            String roleArn = iamClient.getRole(getRoleRequest).getRole().getArn();
            log.info("The role called " + roleName + " already exists.");
            return roleArn;
        } catch (NoSuchEntityException e) {
            log.info("The role called " + roleName + " does not exist. Creating it now.");
            CreateRoleRequest createRoleRequest = new CreateRoleRequest()
                    .withRoleName(roleName)
                    .withAssumeRolePolicyDocument(ASSUME_ROLE_POLICY);

            String roleArn = iamClient.createRole(createRoleRequest).getRole().getArn();

            String rolePolicyStatement = EXPORT_ROLE_S3_STATEMENT_TEMPLATE.replace("{bucket_name}", s3Bucket);

            if (kmsArn != null) {
                rolePolicyStatement = rolePolicyStatement + "," + EXPORT_ROLE_KMS_STATEMENT_TEMPLATE.replace("{kms_arn}", kmsArn);
            }

            String rolePolicy = POLICY_TEMPLATE.replace("{statements}", rolePolicyStatement);
            CreatePolicyResult createPolicyResult = iamClient.createPolicy(new CreatePolicyRequest()
                    .withPolicyName(rolePolicyName)
                    .withPolicyDocument(rolePolicy));

            iamClient.attachRolePolicy(new AttachRolePolicyRequest()
                    .withRoleName(roleName)
                    .withPolicyArn(createPolicyResult.getPolicy().getArn()));

            log.info("Role " + roleName + " created with ARN: " + roleArn + " and policy: " + rolePolicy);
            return roleArn;
        }
    }

    /**
     * Request QLDB to export the contents of the journal for the given time
     * period and s3 configuration. Before calling this function the S3 bucket
     * should be created, see {@link #createS3BucketIfNotExists(String, AmazonS3)}.
     *
     * @param name
     *              Name of the ledger.
     * @param startTime
     *              Time from when the journal contents should be exported.
     * @param endTime
     *              Time until which the journal contents should be exported.
     * @param bucket
     *              S3 bucket to write the data to.
     * @param prefix
     *              S3 prefix to be prefixed to the files written.
     * @param s3EncryptionConfiguration
     *              Encryption configuration for S3.
     * @param roleArn
     *              The IAM role ARN to be used when exporting the journal.
     * @return {@link ExportJournalToS3Result} from QLDB.
     */
    public static ExportJournalToS3Result createExport(String name, Date startTime, Date endTime, String bucket,
                                                       String prefix, S3EncryptionConfiguration s3EncryptionConfiguration,
                                                       String roleArn) {
        log.info("Let's create a journal export for ledger with name: {}...", name);

        S3ExportConfiguration s3ExportConfiguration = new S3ExportConfiguration().withBucket(bucket).withPrefix(prefix)
                .withEncryptionConfiguration(s3EncryptionConfiguration);

        ExportJournalToS3Request request = new ExportJournalToS3Request()
                .withName(name)
                .withInclusiveStartTime(startTime)
                .withExclusiveEndTime(endTime)
                .withS3ExportConfiguration(s3ExportConfiguration)
                .withRoleArn(roleArn);
        try {
            ExportJournalToS3Result result = client.exportJournalToS3(request);
            log.info("Requested QLDB to export contents of the journal.");
            return result;
        } catch (InvalidParameterException ipe) {
            log.error("The eventually consistent behavior of the IAM service may cause this export"
                    + " to fail its first attempts, please retry.");
            throw ipe;
        }
    }

    /**
     * Wait for the JournalS3Export to complete.
     *
     * @param ledgerName
     *              Name of the ledger.
     * @param exportId
     *              Optional KMS ARN used for S3-KMS encryption.
     * @param awaitTimeoutMs
     *              Milliseconds to wait for export to complete.
     * @return {@link DescribeJournalS3ExportResult} from QLDB.
     * @throws InterruptedException if thread is interrupted while busy waiting for JournalS3Export to complete.
     */
    public static DescribeJournalS3ExportResult waitForExportToComplete(String ledgerName, String exportId, long awaitTimeoutMs)
            throws InterruptedException {
        log.info("Waiting for JournalS3Export for " + exportId + "to complete.");
        int count = 0;
        long maxRetryCount = (awaitTimeoutMs / EXPORT_COMPLETION_POLL_PERIOD_MS) + 1;
        while (count < maxRetryCount) {
            DescribeJournalS3ExportResult result = DescribeJournalExport.describeExport(ledgerName, exportId);
            if (result.getExportDescription().getStatus().equalsIgnoreCase(ExportStatus.COMPLETED.name())) {
                log.info("JournalS3Export completed.");
                return result;
            }
            log.info("JournalS3Export is still in progress. Please wait. ");
            Thread.sleep(EXPORT_COMPLETION_POLL_PERIOD_MS);
            count++;
        }
        throw new IllegalStateException("Journal Export did not complete for " + exportId);
    }
}

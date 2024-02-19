package edu.cut.smacc.server.tier;


import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SNSActions;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.*;
import edu.cut.smacc.configuration.ServerConfigurations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This is the notification service where a notification from s3 is handled
 * and being forwarded to Notification processor for further use
 *
 * @author Theodoros Danos
 */
public class NotificationHandler implements Runnable {
    /* STATIC */
    // Shared queue for notifications from HTTP server
    static BlockingQueue<Map<String, String>> messageQueue = new LinkedBlockingQueue<>();
    // Logger
    private static final Logger logger = LogManager.getLogger(NotificationHandler.class);

    /* INSTANCE */
    private volatile boolean shutdown = false;
    private final StringBuilder snsTopicARN = new StringBuilder();
    private String snsTopicARNString;
    private boolean arnAvailalbe = false;
    private String bucketName = null;
    private String keyName = null;
    private String eventName = null;
    private ExecutorService notificationProcessingPool;
    private TierManager tier;

    public NotificationHandler(TierManager tier) {
        this.notificationProcessingPool = Executors
                .newFixedThreadPool(ServerConfigurations.getNotificationProcessingPoolSize());
        this.tier = tier;
    }

    public void shutdown() {
        shutdown = true;
        notificationProcessingPool.shutdown();
    }

    public String getSNSTopicARN() {
        try {
            synchronized (snsTopicARN) {
                if (!arnAvailalbe) snsTopicARN.wait();
            }
        } catch (InterruptedException e) { /* Do nothing */ }

        return snsTopicARNString;
    }

    // Receiver loop
    public void run() {

        // Create a client
        AmazonSNS service = AmazonSNSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        ServerConfigurations.getMasterAccessKey(),
                        ServerConfigurations.getMasterSecretKey())))
                .withEndpointConfiguration(new EndpointConfiguration(
                        ServerConfigurations.getSNSAmazonHostname(),
                        ServerConfigurations.getDefaultRegion()))
                .build();

        CreateTopicResult createRes = null;

        // Create a topic
        try {
            CreateTopicRequest createReq = new CreateTopicRequest().withName(ServerConfigurations.getSNSTopicName());
            createRes = service.createTopic(createReq);
            snsTopicARN.append(createRes.getTopicArn());
            snsTopicARNString = snsTopicARN.toString();
        } catch (SdkClientException ex) {
            logger.error("SNS Service Failed to connect with amazon. Without SNS a client cannot write",
                    ex.getMessage());
            System.exit(1);
            return;
        }

        synchronized (snsTopicARN) {
            arnAvailalbe = true;
            snsTopicARN.notifyAll();    //notify all waiters
        }

        //	Create policy for topic
        if (logger.isDebugEnabled()) logger.info("Topic Created");
        Statement st = new Statement(Effect.Allow)
                .withPrincipals(Principal.AllUsers)
                .withActions(SNSActions.Publish, SNSActions.Subscribe)
                .withResources(new Resource(snsTopicARNString));

        Statement[] statements = new Statement[1];
        statements[0] = st;

        Policy policy = new Policy().withStatements(statements);
        service.setTopicAttributes(snsTopicARNString, "Policy", policy.toJson());

        // Create and start HTTP server
        try {
            // org.mortbay.log.Log.setLog(null);
            Server server = new Server(ServerConfigurations.getSNSLocalPort());
            server.setHandler(new SNSHttpHandler());
            server.start();
        } catch (Exception e) {
            logger.fatal("Unable to start SNS Server: " + e.getMessage());
        }

        try {
            // Subscribe to topic
            String protocol = "http";
            String localEndpoint = ServerConfigurations.getSNSHostnameOrEndpoint();
            if (localEndpoint.startsWith("http")) {
                protocol = localEndpoint.split(":")[0];
            } else {
                localEndpoint = "http://" + localEndpoint + ":" + ServerConfigurations.getSNSLocalPort();
            }

            SubscribeRequest subscribeReq = new SubscribeRequest()
                    .withTopicArn(snsTopicARNString)
                    .withProtocol(protocol)
                    .withEndpoint(localEndpoint);

            //<<<<<<<_------ GET PORT FROM CONFIGURATIONS
            service.subscribe(subscribeReq);
            if (logger.isDebugEnabled())
                logger.info("Topic Subscribed to " + localEndpoint);
        } catch (Exception e) {
            logger.error("SNS Thread[Subscribe to topic error]: " + e.getMessage());
            return;
        }

        //wait for notifications
        try {
            ConfirmSubscriptionResult resultArn = null;
            while (!shutdown) {

                // Wait for a message from HTTP server
                try {
                    Map<String, String> messageMap = messageQueue.take();

                    // Look for a subscription confirmation Token
                    String token = messageMap.get("Token");
                    if (token != null) {
                        if (logger.isDebugEnabled())
                            logger.info("Topic Confirmed | ARN:" + snsTopicARNString);
                        // Confirm subscription
                        ConfirmSubscriptionRequest confirmReq = new ConfirmSubscriptionRequest()
                                .withTopicArn(createRes.getTopicArn())
                                .withToken(token);
                        resultArn = service.confirmSubscription(confirmReq);

                        continue;
                    }

                    // Check for a notification
                    String message = messageMap.get("Message");
                    if (message != null) {
                        if (logger.isDebugEnabled()) logger.info("Received message: " + message);
                        JSONObject jsonResponse;
                        try {
                            jsonResponse = new JSONObject(message);
                        } catch (JSONException ex) {
                            continue;
                        }
                        handleNotification(jsonResponse);
                    }

                } catch (InterruptedException ignored) {
                }
            }

            //	Unsubscribe
            if (resultArn != null)
                service.unsubscribe(resultArn.getSubscriptionArn());

        } catch (Exception e) {
            logger.fatal("SNS Service Failed: " + e.getMessage(), e);
        }
    }

    private void handleNotification(JSONObject jsonResponse) {
        bucketName = null;
        keyName = null;
        eventName = null;

        retrieveBucketKey(jsonResponse);
        if (bucketName != null && keyName != null && eventName != null) {
            //handle event
            NotificationEvent newNotification = new NotificationEvent(bucketName, keyName, eventName);
            notificationProcessingPool.submit(new NotificationProcessor(tier, newNotification));
        }
    }

    private void retrieveBucketKey(JSONObject jsonResponse) {
        if (jsonResponse.has("Records")) {
            JSONArray records = jsonResponse.getJSONArray("Records");
            if (records.length() > 0) {
                for (int i = 0; i < records.length(); i++) {
                    JSONObject jobj = records.getJSONObject(i);
                    if (jobj.has("s3") && jobj.has("eventName")) {
                        JSONObject s3obj = jobj.getJSONObject("s3");
                        if (s3obj.has("bucket") && s3obj.has("object")) {
                            JSONObject bucketInfo = s3obj.getJSONObject("bucket");
                            JSONObject objectInfo = s3obj.getJSONObject("object");
                            bucketName = bucketInfo.getString("name");
                            keyName = objectInfo.getString("key");
                            eventName = jobj.getString("eventName");
                        }
                    }
                }
            }
        }
    }
}



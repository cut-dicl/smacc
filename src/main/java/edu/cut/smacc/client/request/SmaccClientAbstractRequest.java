package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.configuration.ClientConfigurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * An abstract class for the common operations between client requests other than PUT & GET.
 */
public abstract class SmaccClientAbstractRequest implements SmaccClientRequest {

    private static final Logger logger = LogManager.getLogger(SmaccClientAbstractRequest.class);

    protected Socket socket;
    protected DataOutputStream sout;
    protected final BasicAWSCredentials clientCredentials;
    protected final String endPoint;
    protected final String region;

    protected SmaccClientAbstractRequest(BasicAWSCredentials clientCredentials, String endPoint, String region) {
        this.clientCredentials = clientCredentials;
        this.endPoint = endPoint;
        this.region = region;
    }

    @Override
    public boolean initiate() throws IOException {
        boolean requestSuccess = false;
        int retriesMade = 0;
        do {
            try {
                /* Connect/Reconnect to Server */
                socket = ServerDial.connect();
                sout = new DataOutputStream(socket.getOutputStream());
                ServerDial.login(sout, clientCredentials, endPoint, region);

                /* Initiate the request */
                requestSuccess = initiateRequest();

                break;
            } catch (IOException ex) {
                logger.error("Error initiating client request: " + ex.getMessage());
                ServerDial.disconnect(socket);
                retriesMade += 1;
                if (retriesMade > ClientConfigurations.getServerMaxRetries()) throw new IOException(ex);
                try {
                    Thread.sleep(ClientConfigurations.getClientReconnectWaitMs());
                } catch (InterruptedException ignored) {
                }
            }
        }
        while (retriesMade <= ClientConfigurations.getServerMaxRetries());
        return requestSuccess;
    }

    @Override
    public void close() throws IOException {
        ServerDial.disconnect(socket);
    }

    protected abstract boolean initiateRequest() throws IOException;
}

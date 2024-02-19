package edu.cut.smacc.server.tier;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Scanner;

// HTTP handler

/**
 * Handles the sns report from Amazon SNS service (required for s3 notification)
 *
 * @author everest
 */
public class SNSHttpHandler extends AbstractHandler {
    // private static final Logger logger =
    // LogManager.getLogger(SNSHttpHandler.class);

    // Handle HTTP request
    @SuppressWarnings("unchecked")
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

        // Scan request into a string
        Scanner scanner = new Scanner(request.getInputStream());
        StringBuilder sb = new StringBuilder();
        while (scanner.hasNextLine()) {
            sb.append(scanner.nextLine());
        }

        // Build a message map from the JSON encoded message
        InputStream bytes = new ByteArrayInputStream(sb.toString().getBytes());
        Map<String, String> messageMap = (Map<String, String>) new ObjectMapper().readValue(bytes, Map.class);

        // Enqueue message map for receive loop
        NotificationHandler.messageQueue.add(messageMap);

        // Set HTTP response
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        ((Request) request).setHandled(true);
    }
}
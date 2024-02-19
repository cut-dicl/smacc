package edu.cut.smacc.client.request;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.configuration.ClientConfigurations;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

class ServerDial {
    private static SocketAddress findServer() {
        String[] serversList = ClientConfigurations.getServerList();
        return new InetSocketAddress(serversList[0], ClientConfigurations.getServersPort());
    }

    static Socket connect() throws IOException {
        SocketAddress sockaddr = findServer();
        Socket socket = new Socket();
        socket.connect(sockaddr, ClientConfigurations.getClientConnectionTimeout());
        socket.setKeepAlive(true);
        socket.setSoTimeout(ClientConfigurations.getClientReadTimeout());

        return socket;
    }

    static void login(DataOutputStream sout, BasicAWSCredentials credentials, String endPoint, String region) throws IOException {
        sout.write(credentials.getAWSAccessKeyId().length());
        sout.write(credentials.getAWSAccessKeyId().getBytes());
        sout.write(credentials.getAWSSecretKey().length());
        sout.write(credentials.getAWSSecretKey().getBytes());
        sout.write(endPoint.length());
        sout.write(endPoint.getBytes());
        sout.write(region.length());
        sout.write(region.getBytes());
    }

    static void disconnect(Socket socket) {
        try {
            socket.shutdownInput();
            socket.shutdownOutput();
            socket.close();
        } catch (Exception ignored) {
        }
    }
}
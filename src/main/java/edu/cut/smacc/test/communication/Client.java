package edu.cut.smacc.test.communication;

import java.io.OutputStream;
import java.net.Socket;

public class Client {
    public static void main(String[] args) throws Exception {

        Thread t = new Thread(() -> {
            try {
                Server.main(null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        t.start();
        Socket socket = new Socket("127.0.0.1", 8080);
        OutputStream out = socket.getOutputStream();

        for (int i = 0; i < 512 * 1024; i++) {
            out.write(i % 26 + 97);
        }

        out.flush();
        out.close();
        socket.close();
        System.out.println("Client done");
    }
}

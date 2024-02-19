package edu.cut.smacc.test.communication;

import java.io.FileOutputStream;

public class Main {
    public static void main(String[] args) throws Exception {

        FileOutputStream file = new FileOutputStream("512KB");

        for (int i = 0; i < 512 * 1024; i++) {
            file.write(i % 26 + 65);
        }

        file.close();
    }
}

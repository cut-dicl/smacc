package edu.cut.smacc.test.internal;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cut.smacc.server.cache.common.GenericObjectException;
import edu.cut.smacc.server.cloud.CloudInfo;
import edu.cut.smacc.server.tier.CacheOutputStream;
import edu.cut.smacc.server.tier.TierManager;

import java.io.*;

/**
 * This file contains test2 classes that are not used in smacc server but are
 * used only for testing
 *
 * @author Theodoros Danos
 */
class PrintFileT implements Runnable {
    private String bucket;
    private String key;
    private TierManager tier;
    private BasicAWSCredentials clientCredentials;

    PrintFileT(String bucket, String key, TierManager tier, BasicAWSCredentials clientCredentials) {
        this.bucket = bucket;
        this.key = key;
        this.tier = tier;
        this.clientCredentials = clientCredentials;
    }

    public void run() {
        try {
            final int buffsize = 1000;
            int r;
            byte[] ar = new byte[buffsize];
            InputStream in;
            CloudInfo cloudInfo = new CloudInfo("s3.amazonaws.com", "us-east-1", clientCredentials.getAWSAccessKeyId(),
                    clientCredentials.getAWSSecretKey());
            in = tier.read(bucket, key, cloudInfo).getInputStream();
            System.out.println("---------*** READ ***----------");
            r = in.read(ar, 0, buffsize);
            while (r > 0) {
                // try{Thread.sleep(50);}catch(InterruptedException e){}
                System.out.print(new String(ar));
                r = in.read(ar, 0, buffsize);
            }
            in.close();
            System.out.println("---------************----------");
        } catch (IOException e) {
            System.err.println("PrintFileClass Exception: " + e.getMessage());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}

class SaveFileT implements Runnable {
    private String bucket, key, outfile;
    private int buffsize, msdelay;
    private TierManager tier;
    private long start, stop;
    private boolean partialFile;
    private BasicAWSCredentials clientCredentials;

    SaveFileT(String bucket, String key, String outfile, int buffsize, int msdelay, TierManager tier,
            BasicAWSCredentials clientCredentials) {
        this.bucket = bucket;
        this.key = key;
        this.outfile = outfile;
        this.tier = tier;
        this.buffsize = buffsize;
        this.msdelay = msdelay;
        this.partialFile = false;
        this.clientCredentials = clientCredentials;
    }

    SaveFileT(String bucket, String key, String outfile, int buffsize, int msdelay, TierManager tier, long start,
            long stop, BasicAWSCredentials clientCredentials) {
        this.bucket = bucket;
        this.key = key;
        this.outfile = outfile;
        this.tier = tier;
        this.buffsize = buffsize;
        this.msdelay = msdelay;
        this.start = start;
        this.stop = stop;
        this.partialFile = true;
        this.clientCredentials = clientCredentials;
    }

    public void run() {
        boolean read = false;
        while (!read) {
            try {
                int r;
                long total = 0;
                byte[] ar = new byte[buffsize];
                InputStream in;

                CloudInfo cloudInfo = new CloudInfo("s3.amazonaws.com", "us-east-1",
                        clientCredentials.getAWSAccessKeyId(),
                        clientCredentials.getAWSSecretKey());
                if (partialFile)
                    in = tier.read(bucket, key, start, stop, cloudInfo).getInputStream();
                else
                    in = tier.read(bucket, key, cloudInfo).getInputStream();

                OutputStream out = new FileOutputStream(new File(outfile));

                String msgStr;
                if (!partialFile)
                    msgStr = "---------*** [" + outfile + "] SAVING [buffer:" + buffsize + "] ***----------";
                else
                    msgStr = "---------*** [" + outfile + "] SAVING [buffer:" + buffsize + "] [START:" + start
                            + " | STOP:" + stop + "] ***----------";
                System.out.println(msgStr);

                r = in.read(ar);

                while (r > 0) {
                    total += r;
                    try {
                        Thread.sleep(msdelay);
                    } catch (InterruptedException ignored) {
                    }

                    out.write(ar, 0, r);
                    r = in.read(ar);
                }

                System.out.println("Reading Thread: Total Bytes Read: " + total);
                in.close();
                read = true;
                System.out.println("---------****** [" + outfile + "] SAVED ******----------");
            } catch (IOException e) {
                System.err.println("SaveFileClass Exception: " + e.getMessage());
                if (partialFile)
                    break;
            }
            // catch(GenericObjectException e){System.err.println("SaveFileClass
            // GOException: "+e.getMessage()); if (partialFile) break; }
            catch (Exception e) {
                System.err.println("SaveFileT EXCEPTION:" + e.getMessage());
                e.printStackTrace();
                break;
            }
        }
    }
}

class CreateFileT implements Runnable {
    private String bucket, key, filename;
    private boolean async;
    private TierManager tier;
    private int bufferSize;
    private BasicAWSCredentials clientCredentials;
    private long length;

    CreateFileT(String bucket, String key, String filename, boolean async, TierManager tier, int bufferSize,
            BasicAWSCredentials clientCredentials, long length) {
        this.bucket = bucket;
        this.key = key;
        this.filename = filename;
        this.async = async;
        this.tier = tier;
        this.bufferSize = bufferSize;
        this.clientCredentials = clientCredentials;
        this.length = length;
    }

    public void run() {
        try {
            InputStream filein = new FileInputStream(new File(filename));
            int r;
            byte[] ar = new byte[bufferSize];
            double ttime;
            long end;
            CacheOutputStream out;

            long start = System.nanoTime();

            CloudInfo cloudInfo = new CloudInfo("s3.amazonaws.com", "eu-central-1",
                    clientCredentials.getAWSAccessKeyId(),
                    clientCredentials.getAWSSecretKey());
            out = tier.create(bucket, key, async, length, cloudInfo).getCacheOutputStream();
            long createEnd = System.nanoTime();

            System.out.println("Create Time ns: " + (createEnd - start));

            if (out == null)
                throw new IOException("Create failed");

            r = filein.read(ar, 0, bufferSize);

            long writeT = System.nanoTime();
            while (r > 0) {
                if (out.isGoingManual())
                    System.err.println("GOING FOR MANUAL <<<<");

                out.write(ar, 0, r);
                r = filein.read(ar, 0, bufferSize);
            }
            System.out.println("Loop: " + (System.nanoTime() - writeT));

            if (out.isAsync()) {
                out.close();
                end = System.nanoTime();
                ttime = (end - start) / 1000000000.0;
                System.out.println("Time: " + ttime);
                return;
            } else {
                // writeT = System.nanoTime();
                out.uploadLastPart();
                System.out.println("Waiting now for uploading..");
                while (out.isUploading())
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ignored) {
                    }
                // System.out.println("Before Closing: " + (System.nanoTime()-writeT));

                out.close();
            }

            filein.close();

            end = System.nanoTime();
            ttime = (end - start) / 1000000000.0;
            System.out.println("Time: " + ttime);

        } catch (IOException e) {
            System.err.println("CreateFile Class Exception: " + e.getMessage());
        } catch (GenericObjectException e) {
            System.err.println("CreateFile Class GOException: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace(new PrintStream(System.err));
        }

    }
}

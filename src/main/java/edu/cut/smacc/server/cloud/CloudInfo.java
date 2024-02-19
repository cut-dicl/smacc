package edu.cut.smacc.server.cloud;

/**
 * Holds connection information to a cloud storage system
 */
public class CloudInfo {
    String endPoint;
    String region;
    String accessKey;
    String secretKey;

    public CloudInfo(String endPoint, String region, String accessKey, String secretKey) {
        this.endPoint = endPoint;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public String getRegion() {
        return region;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

}

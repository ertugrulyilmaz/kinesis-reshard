package com.us;

public class App {

    public static void main(String[] args) {
        final String accessKey = args[0];
        final String secretKey = args[1];
        final String streamName = args[2];
        final String shardId = args[3];

        KinesisUtility utility = new KinesisUtility(accessKey, secretKey);

        utility.reshard(streamName, shardId);
    }

}

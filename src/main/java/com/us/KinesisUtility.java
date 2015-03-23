package com.us;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.SplitShardRequest;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class KinesisUtility {

    private final AmazonKinesisClient client;

    public KinesisUtility(final String accessKey, final String secretKey) {
        this.client = new AmazonKinesisClient(new BasicAWSCredentials(accessKey, secretKey));
        this.client.setRegion(Region.getRegion(Regions.EU_WEST_1));
    }

    public void reshard(final String streamName, final String shardId) {
        List<Shard> shards = getShards(streamName);

        for (Shard shard : shards) {
            if (shardId.equals(shard.getShardId())) {
                System.out.println(shard.getShardId() + " is resharding...");

                SplitShardRequest splitShardRequest = new SplitShardRequest();
                splitShardRequest.setStreamName(streamName);
                splitShardRequest.setShardToSplit(shard.getShardId());

                BigInteger startingHashKey = new BigInteger(shard.getHashKeyRange().getStartingHashKey());
                BigInteger endingHashKey = new BigInteger(shard.getHashKeyRange().getEndingHashKey());
                String newStartingHashKey = startingHashKey.add(endingHashKey).divide(new BigInteger("2")).toString();

                splitShardRequest.setNewStartingHashKey(newStartingHashKey);

                client.splitShard(splitShardRequest);

                break;
            }
        }
    }

    public List<Shard> getShards(final String streamName) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        List<Shard> shards = new ArrayList<>();

        String exclusiveStartShardId = null;

        do {
            describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
            DescribeStreamResult describeStreamResult = client.describeStream(describeStreamRequest);
            shards.addAll(describeStreamResult.getStreamDescription().getShards());

            if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
                exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
            } else {
                exclusiveStartShardId = null;
            }
        } while (exclusiveStartShardId != null);

        return shards;
    }

}

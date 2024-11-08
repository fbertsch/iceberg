package com.netflix.iceberg.security;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.ExecutorAskRunner;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

public class ExecutorAskListener extends SparkListener {
    private final SparkConf conf;

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        // register the rpc endpoint to the driver rpc env
        SparkEnv.get().rpcEnv().setupEndpoint(ExecutorAskRunner.class.getName(),
                new ExecutorAskRunner(SparkEnv.get().rpcEnv(), conf));
    }

    public ExecutorAskListener(SparkConf sparkConf) {
        this.conf = sparkConf;
    }
}

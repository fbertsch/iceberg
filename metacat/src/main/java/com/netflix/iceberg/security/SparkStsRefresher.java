package com.netflix.iceberg.security;

import com.netflix.s3authsts.common.rest.StsCredentials;
import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.ExecutorAsk;
import org.apache.spark.rpc.ExecutorAskRunner;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.util.RpcUtils;

import java.io.Serializable;
import java.util.function.Supplier;

public class SparkStsRefresher implements Serializable, Supplier<StsCredentials> {
    private SecurityContext securityContext;
    private transient RpcEndpointRef credEndpointRef;

    public SparkStsRefresher(SecurityContext securityContext) {
        this.securityContext = securityContext;
        this.credEndpointRef = buildCredEndpointRef();
    }

    @Override
    public StsCredentials get() {
        if (credEndpointRef == null) {
            credEndpointRef = buildCredEndpointRef();
        }
        return (StsCredentials) credEndpointRef.askSync(
                new SignerAsk(securityContext),
                scala.reflect.ClassTag$.MODULE$.apply(StsCredentials.class));
    }

    private RpcEndpointRef buildCredEndpointRef() {
        return RpcUtils.makeDriverRef(
                ExecutorAskRunner.class.getName(),
                SparkEnv.get().conf(),
                SparkEnv.get().rpcEnv());
    }

    private class SignerAsk extends ExecutorAsk {
        private final SecurityContext securityContext;

        public SignerAsk(SecurityContext securityContext) {
            this.securityContext = securityContext;
        }
        @Override
        public Object run() {
            return securityContext.loadStsCredentials();
        }

        @Override
        public String cacheKey() {
            // here we need to cache by table identifier + creation location to account for the case where a table
            // is deleted and created again under a different S3 prefix, which requires new signer cred scoped
            // for the new prefix. Solely caching by table identifier will lead to S3 access denied error.
            return securityContext.tableIdentifier() + " + " + securityContext.getCreationLocation();
        }
    }
}

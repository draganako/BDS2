package com.spark;
import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

public class CassandraConnector {
    
    private CqlSession session;
    private static CassandraConnector instance;


    public static CassandraConnector getInstance() {
        if (instance == null) {
            instance = new CassandraConnector();
        }
        return instance;
    }

    public void connect(String addr, Integer port) { // 9042
        CqlSessionBuilder builder = CqlSession.builder();
        builder.withAuthCredentials("cassandra", "cassandra")
            .withLocalDatacenter("datacenter1")
            .addContactPoint(new InetSocketAddress(addr, port));

        session = builder.build();
    }

    public CqlSession getSession() {
        return session;
    }

    public void close() {
        session.close();
    }

    private CassandraConnector() {
        
    }
}
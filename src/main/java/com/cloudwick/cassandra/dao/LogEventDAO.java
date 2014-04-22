package com.cloudwick.cassandra.dao;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Data access object for log events for cassandra
 *
 * @author ashrith
 */
public class LogEventDAO {
    private static final Logger logger = LoggerFactory.getLogger(LogEventDAO.class);
    Session session = null;
    Cluster cluster = null;
    String keyspace = null;

    public LogEventDAO(Session session) {
        this.session = session;
    }

    public LogEventDAO() {
        this("127.0.0.1");
    }

    /**
     * Constructs a session object for a given contact node
     * @param contactNode a cassandra node used to fetch the cluster information from
     */
    public LogEventDAO(String contactNode) {
        cluster = Cluster.builder()
                .addContactPoint(contactNode)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .build();

        Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: %s\n", metadata.getClusterName());

        for (Host host: metadata.getAllHosts()) {
            logger.info(
                    "DataCenter: %s, Host: %s, Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        this.connect();
    }

    /**
     * Constructs a session object for a given list of cassandra nodes
     * @param contactNodes list of contact nodes to the cluster information from
     */
    public LogEventDAO(List<String> contactNodes, String key_space) {
        try {
            keyspace = key_space;
            cluster = Cluster.builder()
                    .addContactPoints(contactNodes.toArray(new String[contactNodes.size()]))
                    .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                    .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                    .build();

            Metadata metadata = cluster.getMetadata();
            logger.debug("Connected to cluster: {}", metadata.getClusterName());

            for ( Host host : metadata.getAllHosts() ) {
                logger.debug("DataCenter: {}; Host: {}; Rack: {}\n",
                        host.getDatacenter(), host.getAddress(), host.getRack());
            }
            this.connect();
        } catch (NoHostAvailableException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception in CassandraConnection class: {}", e);
        }
    }

    /**
     * Connects to the cassandra cluster using the provided keyspace
     * @param keyspace
     */
    public void connect(String keyspace) {
        if (session == null) {
            session = cluster.connect(keyspace);
        }
    }

    public void connect() {
        if (session == null) {
            session = cluster.connect();
        }
    }

    /**
     * Returns a session instance of the connection
     * @return session instance
     */
    public Session getSession() {
        return this.session;
    }

    /**
     * Disconnects from cassandra cluster
     */
    public void close() {
        if (session != null) {
            session.close();
            logger.debug("Successfully closed session");
        }
        if (cluster != null) {
            cluster.close();
            logger.debug("Successfully closed cluster connection");
        }
    }

    /**
     * Creates required schema for movie dataset with provided keyspace name
     * @param repFactor replication factor for the keyspace
     */
    public void createSchema(Integer repFactor) {
        getSession().execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
                "WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': %d};", keyspace, repFactor));
        /*
         * Tables for the keyspace
         */
        getSession().execute(String.format("CREATE TABLE IF NOT EXISTS %s.page_views (" +
                "page VARCHAR, views COUNTER, " +
                "PRIMARY KEY(page));", keyspace));
        getSession().execute(String.format("CREATE TABLE IF NOT EXISTS %s.log_volume_by_minute (" +
                "timestamp VARCHAR, count COUNTER, " +
                "PRIMARY KEY(timestamp));", keyspace));
        getSession().execute(String.format("CREATE TABLE IF NOT EXISTS %s.status_counter (" +
                "status_code INT, count COUNTER, " +
                "PRIMARY KEY(status_code));", keyspace));
        getSession().execute(String.format("CREATE TABLE IF NOT EXISTS %s.visits_by_country (" +
                "country VARCHAR, city VARCHAR, count COUNTER, " +
                "PRIMARY KEY(country, city));", keyspace));
    }

    public void updatePageViews(String page_url, Integer count) {
        getSession().execute(String.format(
                "UPDATE %s.page_views SET views = views + %d WHERE page='%s'", keyspace, count, page_url));
    }

    public void updateLogVolumeByMinute(String timestamp, Integer count) {
        getSession().execute(String.format(
                "UPDATE %s.log_volume_by_minute SET count = count + %d WHERE timestamp='%s'",
                keyspace, count, timestamp));
    }

    public void updateStatusCounter(Integer status_code, Integer count) {
        getSession().execute(String.format(
                "UPDATE %s.status_counter SET count = count + %d WHERE status_code=%d",
                keyspace, count, status_code));
    }

    public void updateVisitsByCountry(String country, String city, Integer count) {
        getSession().execute(String.format(
                "UPDATE %s.visits_by_country SET count = count + %d WHERE country='%s' AND city='%s'",
                keyspace, count, country, city));
    }

    public void findCQLByQuery(String CQL) {
        Statement cqlQuery = new SimpleStatement(CQL);
        cqlQuery.setConsistencyLevel(ConsistencyLevel.ONE);
        cqlQuery.enableTracing();

        ResultSet resultSet = getSession().execute(cqlQuery);

        // Get the columns returned by the query
        ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
        // logger.debug(columnDefinitions.toString());
        for (Row row: resultSet) {
            logger.debug(row.toString());
        }
    }

    /**
     * Drops the specified keyspace
     */
    public void dropSchema() {
        getSession().execute("DROP KEYSPACE " + keyspace);
        logger.info("Finished dropping " + keyspace + " keyspace.");
    }

    /**
     * Drops a specified table from a given keyspace
     * @param table to delete
     */
    public void dropTable(String table) {
        getSession().execute("DROP TABLE IF EXISTS " + keyspace + "." + table);
        logger.info("Finished dropping table " + table + " from " + keyspace + " keyspace.");
    }
}
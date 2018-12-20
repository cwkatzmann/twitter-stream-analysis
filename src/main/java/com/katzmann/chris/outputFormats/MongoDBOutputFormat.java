package com.katzmann.chris.outputFormats;

import com.katzmann.chris.dto.TwitterSentiments;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.io.OutputFormat;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.util.Arrays;


public class MongoDBOutputFormat implements OutputFormat<TwitterSentiments> {

    private MongoClient mongoClient;
    private MongoCollection mongoCollection;
    private ConnectionString connStr;
    private String hostAddress;
    private int port;
    private String username;
    private String password;
    private String databaseName;
    private String collectionName;

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBOutputFormat.class);

    @Override
    public void configure(Configuration params) {
    }

    @Override
    public void open(int taskNum, int numTasks) {
        establishConnection();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
        mongoCollection = mongoDatabase.getCollection(collectionName);
    }

    private void establishConnection() {

        if (connStr != null) {
            mongoClient = MongoClients.create(connStr);
        } else if (username != null) {
            MongoCredential credential = MongoCredential.createCredential(
                    username, databaseName, password.toCharArray());
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyToClusterSettings(builder -> builder.hosts(
                            Arrays.asList(new ServerAddress(hostAddress, port))))
                    .credential(credential)
                    .build();

            mongoClient = MongoClients.create(settings);
        } else {
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyToClusterSettings(builder -> builder.hosts(
                            Arrays.asList(new ServerAddress(hostAddress, port))))
                    .build();

            mongoClient = MongoClients.create(settings);
        }
    }

    @Override
    public void writeRecord(TwitterSentiments record) {
        Document document = new Document()
                .append("positive", record.positive)
                .append("neutral", record.neutral)
                .append("negative", record.negative);


        mongoCollection.insertOne(document);
        LOG.info("Wrote document to MongoDB.");
    }

    @Override
    public void close() {

        if (this.mongoClient != null) {
            this.mongoClient.close();
            this.mongoClient = null;
        }
    }

    public static MongoDBOutputFormatBuilder builder() {
        return new MongoDBOutputFormatBuilder();
    }


    public static class MongoDBOutputFormatBuilder {

        private final MongoDBOutputFormat format;

        protected MongoDBOutputFormatBuilder() {
            format = new MongoDBOutputFormat();
        }

        public MongoDBOutputFormatBuilder setDatbaseName(String databaseName) {
            format.databaseName = databaseName;
            return this;
        }

        public MongoDBOutputFormatBuilder setCollectionName(String collectionName) {
            format.collectionName = collectionName;
            return this;
        }

        public MongoDBOutputFormatBuilder setConnStr(String connStr) {
            format.connStr = new ConnectionString(connStr);
            return this;
        }

        public MongoDBOutputFormatBuilder setHostAddress(String hostAddress) {
            format.hostAddress = hostAddress;
            return this;
        }

        public MongoDBOutputFormatBuilder setPort(Integer port) {
            format.port = port;
            return this;
        }

        public MongoDBOutputFormatBuilder setUsername(String username) {
            format.username = username;
            return this;
        }

        public MongoDBOutputFormatBuilder setPassword(String password) {
            format.password = password;
            return this;
        }

        public MongoDBOutputFormat finish() {

            if (format.databaseName == null) {
                LOG.error("Database name not supplied, aborting");
                throw new IllegalArgumentException("No database name supplied.");
            }

            if (format.collectionName == null) {
                LOG.error("Collection name not supplied, aborting.");
                throw new IllegalArgumentException("No collection name supplied.");
            }

            if (format.connStr != null) {
                return format;
            }

            if (format.password == null) {
                LOG.info("Password not supplied.");
            }

            if (format.username == null) {
                LOG.info("Username not supplied.");
            }

            if (format.hostAddress == null) {
                LOG.error("host address not supplied, aborting");
                throw new IllegalArgumentException("No host address supplied.");
            }

            return format;
        }
    }
}

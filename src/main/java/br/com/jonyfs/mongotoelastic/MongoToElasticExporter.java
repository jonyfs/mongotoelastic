package br.com.jonyfs.mongotoelastic;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.mongodb.core.DocumentCallbackHandler;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

@Component
public class MongoToElasticExporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoToElasticApplication.class);

    @Resource
    MongoTemplate mongoTemplate;

    @Resource
    ElasticSearchTransporter elasticSearchTransporter;

    @Resource
    ElasticsearchTemplate elasticsearchTemplate;

    public void run() {
        int limit = 1000;
        int skip;
        LOGGER.info("Starting...");
        for (String collectionName : mongoTemplate.getCollectionNames()) {
            skip = 0;
            LOGGER.info("Looping collection {} ", collectionName);
            DocumentCallbackHandler documentCallbackHandler = new DocumentHandler(elasticsearchTemplate, collectionName);

            Query query = new Query();
            long total = mongoTemplate.count(query, collectionName);
            LOGGER.info("{} documents. Processing...", total);
            while (skip <= total) {

                query.skip(skip).limit(limit);

                try {
                    LOGGER.info("Query {}  with skip({}).limit({})", query, skip, limit);
                    mongoTemplate.executeQuery(query, collectionName, documentCallbackHandler);
                } catch (Exception e) {
                    LOGGER.error("Failure!", e);
                }
                skip += limit;
            }
        }
    }

}

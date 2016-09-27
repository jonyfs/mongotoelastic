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

        LOGGER.info("Starting...");
        for (String collectionName : mongoTemplate.getCollectionNames()) {
            LOGGER.info("Looping collection {} ", collectionName);
            DocumentCallbackHandler documentCallbackHandler = new DocumentHandler(elasticsearchTemplate, collectionName);
            Query query = new Query();
            query.limit(1000);
            mongoTemplate.executeQuery(query, collectionName, documentCallbackHandler);
        }
    }

}

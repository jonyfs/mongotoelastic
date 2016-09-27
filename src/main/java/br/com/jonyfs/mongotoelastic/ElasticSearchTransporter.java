package br.com.jonyfs.mongotoelastic;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.mongodb.core.DocumentCallbackHandler;
import org.springframework.stereotype.Component;

import com.mongodb.DBObject;
import com.mongodb.MongoException;

@Component
public class ElasticSearchTransporter implements DocumentCallbackHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchTransporter.class);

    @Resource
    ElasticsearchTemplate elasticsearchTemplate;

    @Override
    public void processDocument(DBObject dbObject)  throws MongoException, DataAccessException{
        LOGGER.info("Processing {}", dbObject.get("_id"));
        //LOGGER.info("Processing {}", dbObject);
        
         //if (elasticsearchTemplate.indexExists(dbObject)
        
        //elasticsearchTemplate.createIndex(indexName)
    }

}

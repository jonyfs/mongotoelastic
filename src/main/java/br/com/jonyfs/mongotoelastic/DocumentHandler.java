package br.com.jonyfs.mongotoelastic;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.mongodb.core.DocumentCallbackHandler;

import com.google.gson.Gson;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class DocumentHandler implements DocumentCallbackHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentHandler.class);

    private final ElasticsearchTemplate elasticsearchTemplate;
    private final String collection;

    private final Gson gson = GsonTypeAdapter.getGsonBuilder(GsonTypeAdapter.GsonAdapterType.SERIALIZER).create();

    public DocumentHandler(ElasticsearchTemplate elasticsearchTemplate, String collection) {
        this.elasticsearchTemplate = elasticsearchTemplate;
        this.collection = collection;
    }

    @Override
    public void processDocument(DBObject dbObject) throws MongoException, DataAccessException {
        LOGGER.info("Processing {}", dbObject.get("_id"));
        if (elasticsearchTemplate.indexExists(collection.toLowerCase())) {
            LOGGER.debug("collection {} exists", this.collection.toLowerCase());
        } else {
            elasticsearchTemplate.createIndex(this.collection.toLowerCase());
        }

        String id = dbObject.get("_id").toString();

        dbObject.removeField("_id");
        String object = gson.toJson(dbObject);

        BulkRequestBuilder bulkRequest = elasticsearchTemplate.getClient().prepareBulk();

        bulkRequest.add(elasticsearchTemplate.getClient().prepareIndex(this.collection.toLowerCase(), this.collection.toLowerCase(), id)
                .setSource(object));

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            LOGGER.error("Failure: {} ", bulkResponse.buildFailureMessage());
        }
    }
}

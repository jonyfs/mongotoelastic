package br.com.jonyfs.mongotoelastic;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.mongodb.core.MongoTemplate;

@SpringBootApplication
public class MongoToElasticApplication implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoToElasticApplication.class);

    @Resource
    MongoTemplate mongoTemplate;

    @Resource
    ElasticsearchTemplate elasticsearchTemplate;

    @Resource
    MongoToElasticExporter mongoToElasticExporter;

    public static void main(String[] args) {
        SpringApplication.run(MongoToElasticApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        LOGGER.info("Verifying application.properties configuration...");

        assert (mongoTemplate != null) : "Problem to connect to mongodb, see http://docs.spring.io/spring-boot/docs/current/reference/html/common-application-properties.html";

        assert (elasticsearchTemplate != null) : "Problem to connect to elasticsearch, see http://docs.spring.io/spring-boot/docs/current/reference/html/common-application-properties.html";

        LOGGER.info("application.properties ok.");

        mongoToElasticExporter.run();
    }
}

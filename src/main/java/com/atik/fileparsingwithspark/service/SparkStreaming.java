package com.atik.fileparsingwithspark.service;

import com.atik.fileparsingwithspark.config.SparkConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.singletonList;

@Configuration
@PropertySource("classpath:application.properties")
public class SparkStreaming {

    private final static Logger logger = Logger.getLogger(SparkStreaming.class);
    public final static String COLLECTION_REGEX = "parsingRegex";

    private static Set<Document> userInfoDocumentList = new HashSet<>();
    private static Set<Document> apiInfoDocumentList = new HashSet<>();

    private static MongoTemplate mongoTemplate;


    @Value("${brokerList}")
    private String brokers;

    @Value("${topic.path}")
    private String topicPath;

    @Value("${streaming.duration.seconds}")
    private long streamingDurationSeconds;

    public SparkStreaming() {
    }


    public void startSparkStreaming(MongoTemplate testDbMongoTemplate) throws InterruptedException {
        mongoTemplate = testDbMongoTemplate;

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("fileParsingWithSpark")
                .set("spark.driver.allowMultipleContexts", "true");


        JavaStreamingContext jssc = new JavaStreamingContext(
                sparkConf,
                Durations.seconds(streamingDurationSeconds));

        HashSet<String> topicsSet = new HashSet<>(
                Collections.singletonList(topicPath));

        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

         JavaPairInputDStream<String, String> messages =
                KafkaUtils.createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicsSet
                );


        JavaDStream<String> lines = messages.map((Function<Tuple2<String, String>, String>) Tuple2::_2);

        lines.foreachRDD(rdd -> {

            rdd.foreach(userParams -> {

                fileProcessing(userParams);

            });


        });

        lines.print();
        jssc.start();
        jssc.awaitTermination();
    }

    public static void insertOrUpdateDocuments(List<Document> documentList, String collectionName) {

        for(Document document : documentList) {
            if(document.get("_id") != null) {
                Query query = new Query(Criteria.where("_id").is(document.get("_id")));

                Update update = Update.fromDocument(document);
                mongoTemplate.upsert(query, update, collectionName);
            } else {
                mongoTemplate.save(document, collectionName);
            }
        }
    }


    private static void fileProcessing(String userParams) {
        String[] pathArray = userParams.split(",");
        String filePath = pathArray[0],
                token = UUID.randomUUID().toString(),
                modelName = pathArray[1];

        apiInfoDocumentList.add(new Document(){
            {
                put("insertionToken", token);
                put("modelName", modelName);
                put("filePath", filePath);
            }
        });

        Document queryDocument = new Document();
        queryDocument.put("modelName", modelName);

        Document filterDocument = new Document("$match", queryDocument);

        JavaMongoRDD<Document> javaMongoRDD = MongoSpark.load(
                SparkConfiguration.getJavaSparkContext(),
                SparkConfiguration.getReadConfig(COLLECTION_REGEX)).withPipeline(Collections.singletonList(filterDocument));

        javaMongoRDD.foreach(eachModel -> {
            Document primaryDocs = new Document();

            Document collection = (Document) eachModel.get("collection");

            String regex = eachModel.get("regex").toString();
            JavaRDD<String> fileData = SparkConfiguration.getJavaSparkContext().wholeTextFiles(filePath).map(tuple -> tuple._2());

            StringBuilder stringBuilder = new StringBuilder();


            fileData.foreach(word-> {
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(word);
                if(matcher.find()) {
                    createCollection(new StringBuilder(matcher.group()), collection, primaryDocs, token);

                    stringBuilder.append(word.replace(matcher.group(), ""));
                    if(stringBuilder.length() > 0) {
                        parseFile(stringBuilder, regex, collection, new Document(), token);
                    }

                }
            });


        });

    }


    private static void parseFile(StringBuilder fileData, String regex, Document collection, Document primaryDocs, String token) throws JsonProcessingException {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(fileData.toString());
        StringBuilder stringBuilder = new StringBuilder();

        if(matcher.find()) {
            stringBuilder.append(fileData.toString().replace(matcher.group(), ""));
            createCollection(new StringBuilder(matcher.group()), collection, primaryDocs, token);

            if(stringBuilder.length() > 0) {
                parseFile(stringBuilder, regex, collection, new Document(), token);
            }

        }
    }

    private static String parseText(String recordParameter, String text) throws JsonProcessingException {
        Pattern patternRecordParameter = Pattern.compile(recordParameter);
        Matcher matcherRecordParameter = patternRecordParameter.matcher(text);
        if(matcherRecordParameter.find())
            return matcherRecordParameter.group();
        else
            return "";
    }

    private static void createCollection(StringBuilder eachFilteredLine, Document collection, Document primaryDocs, String token) throws JsonProcessingException {

        ArrayList<Document> records = (ArrayList<Document>) collection.get("records");

        Document document = new Document();
        Document primary = new Document();
        StringBuilder filteredLine = new StringBuilder();
        List<String> recordParameters = new ArrayList<>();
        List<String> primaryValues = new ArrayList<>();

        for (Document record : records) {
            document.put("insertionToken", token);
            ArrayList<Document> properties = (ArrayList<Document>) record.get("properties");

            String recordParameter = record.get("recordParameter").toString();
            String text = parseText("^"+recordParameter, eachFilteredLine.toString());

            if(text.isEmpty()) {
                text = parseText(recordParameter, eachFilteredLine.toString());
                if(!text.isEmpty()) {
                    filteredLine.append(eachFilteredLine.toString().replace(text, ""));
                    recordParameters.add(recordParameter);
                }

            }

            for (Document property : properties) {

                if (property.get("propertyName") != null) {

                    ArrayList<Document> rules = (ArrayList<Document>) property.get("rules");

                    String value = "";
                    boolean isEligibleProperty = true;

                    for (Document rule : rules) {

                        String type = rule.get("type").toString();

                        if (type.equals("trim")) {
                            Pattern pattern = Pattern.compile(rule.get("regex").toString());
                            Matcher matcher = pattern.matcher(value);
                            if (matcher.find())
                                value = matcher.group();
                        } else if (type.equals("condition")) {
                            if (!value.matches(rule.get("regex").toString())) {
                                isEligibleProperty = false;
                                break;
                            }
                        } else if (type.equals("defaultValue")) {
                            if (value.matches(rule.get("regex").toString()))
                                value = rule.get("defaultValue").toString();
                        } else {
                            Pattern pattern = Pattern.compile(rule.get("regex").toString());
                            Matcher matcher = pattern.matcher(text);
                            if (matcher.find())
                                value = matcher.group();
                        }

                    }

                    if (isEligibleProperty) {
                        if (property.get("isPrimary") != null && property.getBoolean("isPrimary") && !value.isEmpty()) {
                            primaryValues.add(property.get("propertyName").toString());
                            primary.put(property.get("propertyName").toString(), value);
                        }
                        document.put(property.get("propertyName").toString(), value);
                    }
                }
            }

        }

        if(primaryValues.size() > 0) {
            String collectionName = (String) collection.get("collectionName");
            document.put("primary", primaryValues);
            primaryDocs.put(collectionName, primary);

            switch (collectionName) {
                case "userInfo":
                    userInfoDocumentList.add(updateCreation(document, collectionName, token));
                    break;
            }

            boolean isParsable = false;

            if(recordParameters != null && recordParameters.size() > 0) {
                for(String recordParameter : recordParameters) {
                    if(!parseText(recordParameter, filteredLine.toString()).isEmpty()) {
                        isParsable = true;
                        break;
                    }
                }
            }

            if(isParsable) {
                createCollection(filteredLine, collection, primaryDocs, token);
            }

            if (collection.get("collection") != null) {
                createCollection(eachFilteredLine, (Document) collection.get("collection"), primaryDocs, token);
            }

            saveData();
        }

    }

    private static Document updateCreation(Document document, String collectionName, String token) throws JsonProcessingException {
        Document queryDocument = new Document();
        for(String primary : (ArrayList<String>)document.get("primary")) {

            queryDocument.put(primary, document.get(primary));
        }

        Document filterDocument = new Document("$match", queryDocument);
        JavaMongoRDD<Document> javaMongoRDD = MongoSpark.load(
                SparkConfiguration.getJavaSparkContext(),
                SparkConfiguration.getReadConfig(collectionName)).withPipeline(singletonList(filterDocument));


        try {
            Document foundDocument = javaMongoRDD.first();
            if(foundDocument != null && foundDocument.size() > 0) {
                document.put("_id", foundDocument.get("_id"));
                foundDocument.remove("_id");
                document.put("oldData", foundDocument);
                document.put("updatedToken", token);
                document.remove("insertionToken");
            }

        } catch (Exception e) {
            System.out.println("Inside Exception==============");
        }

        return document;
    }

    private static void saveData() {
        if (userInfoDocumentList.size() != 0) {
            insertOrUpdateDocuments(new ArrayList<>(userInfoDocumentList), "userInfo");
            userInfoDocumentList.clear();
        }
    }
}
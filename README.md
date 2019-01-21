# FILE PARSING WITH SPARK
#### Purpose/Objective Summary: 
         Determining the user data(for using another application) based on user specific information which comes from API end-point(file). 
         And finally store it into MongoDB(collection name is 'userInfo'). This is a POC project for parsing a file with efficent way.

#### Technology Used:
         
       1. Spring Boot 2.0.1.RELEASE (For creating Rest API End-Point)
       2. JDK 1.8
       3. Apache Kafka ( kafka_2.11-2.0.0.tgz)
       4. Apache Spark (spark-2.2.1-bin-hadoop2.7.tgz)
       5. Maven 3.3.3
       6. MongoDB 3.4
       7. Tomcat 7

#### Installation Process For Linux Machine:

       1. JDK install:
             
             Atfirst need to download jdk-1.8 version.
             Then extract zip file and put extract file into usr directory and renaming it to java.
             Then add java environment and path variable to /etc/profile file.
             Then check java version command is - "java -version". If it shows version then install process is ok.

       2. Maven install:

             Atfirst need to download apache-maven-3.3.3.rar.
             Then extract zip file and put extract file into usr directory and renaming it to maven.
             Then add maven environment and path variable to /etc/profile file.
             Then check java version command is - "mvn -version". If it shows version then install process is ok.

       2. Kafka install:

            Atfirst you need to download kafka_2.11-2.0.0.tgz binary file.
            Then extract command is - "tar xzf kafka_2.11-2.0.0.tgz" and put extract file into usr directory command is - 
            "sudo mv kafka_2.11-2.0.0 /usr/" with renaming kafka instead of kafka_2.11-2.0.0.
            Open a file name is config/server.properties and Now make changes (check if they are set) to following properties:

            broker.id=0
            listeners=PLAINTEXT://:9092
            num.partitions=1
            log.dirs=/tmp/kafka-logs-0

       3. Spark install:
         
            Atfirst you need to download spark-2.2.1-bin-hadoop2.7.tgz binary file.
            Then extract command is - "tar xzf spark-2.2.1-bin-hadoop2.7.tgz" and put extract file into usr directory command is - 
            "sudo mv spark-2.2.1-bin-hadoop2.7 /usr/" with renaming spark instead of spark-2.2.1-bin-hadoop2.7.

       4. MongoDB install:

            MongoDB installation process link is given below -
            https://docs.mongodb.com/manual/administration/install-on-linux/

       5. Tomcat install: 
            
            No need to install tomcat manually because here using spring boot embeded tomcat server.

#### Need to follow Step by Step

   Need to Start Everything UP:
   
       1. Need to start MongoDB command is - "sudo service mongod start"
       2. Log in as root
       3. Go to the kafka directory. Command is - "cd /usr/kafka"
       4. First you need to start the zookeeper, it will be used to store the offsets for topics. 
          Command is - "./bin/zookeeper-server-start.sh ./config/zookeeper.properties"
       5. Open another terminal and follow the step 2 & 3
       6. Then need to start kafka server. Command is - "./bin/kafka-server-start.sh ./config/server.properties"
       7. Open another terminal and follow the step 2 & 3
       8. Then creating a topic (need only once). Command is - 
          "bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic path"


   Data Model:

       Need to create a collection name is 'parsingRegex'. Actually file parsing depends on this regex document. So please add below document for example - 

```json
{ 
    "modelName" : "testExample", 
    "regex" : "(?s)200~.*?(?=(200~)|$(?!(200~)))", 
    "collection" : {
        "collectionName" : "userInfo", 
        "records" : [
            {
                "recordParameter" : "200~.*", 
                "properties" : [
                    {
                        "propertyName" : "name", 
                        "rules" : [
                            {
                                "type" : "fetchPrefix", 
                                "operationType" : "execute", 
                                "startWith" : "~01", 
                                "endWith" : "~", 
                                "regex" : "(?<=~01).*?(?=~)"
                            }
                        ]
                    }, 
                    {
                        "propertyName" : "mailCode", 
                        "rules" : [
                            {
                                "type" : "fetchPrefix", 
                                "operationType" : "execute", 
                                "startWith" : "~02", 
                                "endWith" : "~", 
                                "regex" : "(?<=~02).*?(?=~)"
                            }
                        ]
                    }, 
                    {
                        "propertyName" : "ssn", 
                        "isPrimary" : true, 
                        "rules" : [
                            {
                                "type" : "fetchPrefix", 
                                "operationType" : "execute", 
                                "startWith" : "~03", 
                                "endWith" : "~", 
                                "regex" : "(?<=~03).*?(?=~)"
                            }
                        ]
                    }, 
                    {
                        "propertyName" : "city", 
                        "rules" : [
                            {
                                "type" : "fetchPrefix", 
                                "operationType" : "execute", 
                                "startWith" : "~04", 
                                "endWith" : "~", 
                                "regex" : "(?<=~04).*?(?=~)"
                            }
                        ]
                    }, 
                    {
                        "propertyName" : "state", 
                        "rules" : [
                            {
                                "type" : "fetchPrefix", 
                                "operationType" : "execute", 
                                "startWith" : "~05", 
                                "endWith" : "~", 
                                "regex" : "(?<=~05).*?(?=~)"
                            }
                        ]
                    }, 
                    {
                        "propertyName" : "zip", 
                        "rules" : [
                            {
                                "type" : "fetchPrefix", 
                                "operationType" : "execute", 
                                "startWith" : "~06", 
                                "endWith" : "~", 
                                "regex" : "(?<=~06).*?(?=~)"
                            }
                        ]
                    }, 
                    {
                        "propertyName" : "mail", 
                        "rules" : [
                            {
                                "type" : "fetchPrefix", 
                                "operationType" : "execute", 
                                "startWith" : "~07", 
                                "endWith" : "~", 
                                "regex" : "(?<=~07).*?(?=~)"
                            }
                        ]
                    }
                ]
            }
        ]
    }
}
```


    Project Run:

       1. Go through the application.properties file(fileparsingwithspark/src/main/resources). 
          Please make sure your DB name and everything look like same as you see here.

       2. Go to the project directory and run this command - "mvn spring-boot:run".

       3. Then go to the browser and type http://localhost:8080/swagger-ui.html

#### API Sample:

   Add modelName, filePathUrl to Kafka Producer:

            [PUT] http://localhost:8080/fileparsing/api/v1/pathUrl?modelName=testExample

            {
              "message": "Message sent to the Kafka 'path' Topic Successfully." 
            }

   ***Here modelName would be a similar with parsingRegex collections modelName. 
   And filePathUrl would be the whole path url of the file which you want to purse. 
   I have given a sample file attach with this project name is 'testData' under fileparsingwithspark/sampleData directory.

Hopefully it will be helpful for understanding. If there's anything you need help with, don't hesitate to contact me. 

  
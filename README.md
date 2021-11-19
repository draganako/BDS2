## Struktura projekta

Projekat predstavlja aplikacije napisane u programskom jeziku Java za analizu stream podataka o letovima širom Amerike. Aplikacija se izvršava
na klasteru Docker kontejnera koji se pokreću na osnovu gotovih image-a. Struktura projekta se može videti u nastavku. 

Direktorijum `config`, kao i datoteke `Dockerfile`, `build-image.sh`, `LICENSE`, `README.md` i skripte `resize-cluster.sh` i `start-container.sh` vezane su za [korišćeni 
Hadoop klaster](https://github.com/kiwenlau/hadoop-cluster-docker). U fajlu `.project` nalazi se opis projekta, dok `docker-compose.yaml` i 
`docker-compose-streaming.yaml` služe za opis korišćenih servisa.
Folderi `stream-producer` i `stream-consumer` su kreirani u okruženju Eclipse kao Maven projekti. Oni sadrže folder `src/main/java/com/spark`,
 gde se nalazi odgovarajuća aplikacija (`App.java`), kao i pomoćne klase. Datoteka `.classpath` sadrži putanje korisnički definisanih klasa,
 paketa i resursa projekta, dok `.project` sadrži njegove build parametre. U datoteci `pom.xml` nalaze se informacije o projektu, kao što
 su servisi koji se koriste, dodaci za kompajliranje i dr. Folder `.settings` namenjen je preferencama projekta. Za pokretanje aplikacija u docker kontejneru dodate su i datoteke `Dockerfile` i `start.sh`.

```bash
hadoop-cluster-docker2/
 |-- config/
 |   |--...
 |-- stream-consumer/
 |---- .settings/
 |     |--...
 |---- src/
 |------ main\java\com\spark/
 |       |--App.java
 |       |--CassandraConnector.java
 |       |--FedsData.java
 |------ test/
 |       |--...
 |     .classpath
 |     .project
 |     Dockerfile
 |     pom.xml
 |     start.sh
 |-- stream-producer/
 |---- .settings/
 |     |--...
 |---- src/
 |------ main\java\com\spark/
 |       |--App.java
 |------ test/
 |       |--...
 |     .classpath
 |     .project
 |     Dockerfile
 |     pom.xml
 |     start.sh
 |.project
 |build-image.sh
 |docker-compose.yaml
 |docker-compose-streaming.yaml
 |Dockerfile
 |LICENSE
 |README.md
 |resize-cluster.sh
 |start-container.sh
```
## Korišćeni skup podataka

Podaci nad kojima treba izvršiti analizu preuzeti su sa [ove lokacije](https://github.com/BuzzFeedNews/2016-04-federal-surveillance-planes/tree/master/data/feds) i spojeni u fajl `feds.csv`, sa više od 120000 stavki. 
Ove stavke predstavljaju podatke prikupljene o praćenim letelicama u Americi u periodu od 4 meseca, pri čemu je [ovde](https://buzzfeednews.github.io/2016-04-federal-surveillance-planes/analysis.html) objašnjeno koja su značenja različitih tipova podataka koji su prisutni.
Prvu vrstu dokumenta čine nazivi odgovarajućih podataka, a ostale njihove vrednosti. 

## Infrastruktura

Aplikacija se izvršava na lokalnom klasteru Docker kontejnera. Hadoop klaster čini master i dva slave-a,
koji se pokreću u svojim kontejnerima (postupak opisan u `hadoop-cluster-docker2/README.md`). 
Na kreiranoj mreži `hadoop` se pokreću i
ostali kontejneri, najpre spark master i worker, opisani u `docker-compose.yaml` 
([Big Data Europe](https://hub.docker.com/u/bde2020)), 
a zatim `cassandra` (zvanični docker image), kafka i zookeeper 
([wurstmeister](https://hub.docker.com/u/wurstmeister) image), `stream-producer` i `stream-consumer`, 
svi definisani u `docker-compose-streaming.yaml`. Pre pokretanja aplikacije potrebno je uneti podatke u HDFS.

**docker-compose.yaml:**

```
version: "3"

services:
  # SPARK
  spark-master:
    image: bde2020/spark-master:2.4.3-hadoop2.7
    container_name: spark-master
    ports:
      - "8080:8080"
      - "7077:7077"
    environment:
      - INIT_DAEMON_STEP=false
  spark-worker-1:
    image: bde2020/spark-worker:2.4.3-hadoop2.7
    container_name: spark-worker-1
    depends_on:
      - spark-master
    environment:
      - "SPARK_MASTER=spark://spark-master:7077"
      
networks:
  default:
    external:
      name: hadoop

```

**docker-compose-streaming.yaml:**

```
version: "3"

services:
  # BigData2 - Spark streaming
  stream-consumer:
    build: ./stream-consumer
    image: stream-consumer:latest
    container_name: stream-consumer
    depends_on: 
      - kafka
      - cassandra
      - stream-producer
    environment:
      INITIAL_SLEEP_TIME_IN_SECONDS: 40
      SPARK_MASTER_NAME: spark-master
      SPARK_MASTER_PORT: 7077
      SPARK_APPLICATION_ARGS: ''
      CASSANDRA_URL: cassandra
      CASSANDRA_PORT: 9042
      KAFKA_URL: kafka:9092
      ENABLE_INIT_DAEMON: 'false'
      DATA_RECEIVING_TIME_IN_SECONDS: 30
  stream-producer:
    build: ./stream-producer
    image: stream-producer:latest
    container_name: stream-producer
    depends_on: 
      - kafka
      - cassandra
    environment:
      INITIAL_SLEEP_TIME_IN_SECONDS: 20
      PUBLISH_INTERVAL_IN_SEC: 5
      HDFS_URL: hdfs://hadoop-master:9000
      CSV_FILE_PATH: /big-data/data.csv
      KAFKA_URL: kafka:9092

  # CASSANDRA
  cassandra:
    image: cassandra
    container_name: cassandra
    expose:
      - "9042"
    ports:
      - "9042:9042"
    volumes:
      - cassandra_data:/var/lib/cassandra 

  # KAFKA
  zookeeper:
    image: wurstmeister/zookeeper:3.4.6
    container_name: zookeeper
    ports:
      - "2181:2181"
  kafka:
    image: wurstmeister/kafka:2.12-2.4.0
    container_name: kafka
    expose:
      - "9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_HOST_NAME: kafka

volumes: 
  cassandra_data:
  
networks:
  default:
    external:
      name: hadoop
```

## Unos podataka

Pomenute podatke treba preuzeti [ovde](https://github.com/BuzzFeedNews/2016-04-federal-surveillance-planes/tree/master/data/feds) i prekopirati ih 
u Hadoop mnt direktorijum:

* `docker cp /<local_repo>/<data_file_name> hadoop-master:/mnt/data.csv`

Zatim u konzoli hadoop master-a treba kreirati folder big-data i uneti prethodno pomenutu datoteku:

* `hdfs dfs -mkdir /big-data`
* `hdfs dfs -put /mnt/data.csv /big-data/data.csv`

Sada je fajl spreman za preuzimanje sa HDFS-a i obradu.

## Pokretanje aplikacije

Nakon pokretanja hadoop kontejnera, treba pokrenuti i ostale:

* `docker-compose -f docker-compose.yaml up -d --build --force-recreate`
* `docker-compose -f docker-compose-streaming.yaml up -d --build --force-recreate`

## Rad aplikacije

Skripta `start.sh` u svakom od foldera aplikacija pokreće gotovu `template.sh` skriptu za pokretanje njihovih
kontejnera.

### Stream producer

Aplikacija `stream-producer` čita jednu po jednu liniju skupa podataka sa HDFS-a i šalje je na
Kafka `feds` topic.  

### Stream consumer

Aplikacija `stream-consumer` čita podatke sa `feds` topic-a i pronalazi agenciju sa najviše dokumentovanih segmenata
letova u datom vremenskom prozoru i maksimalnu i minimalnu brzinu za određenu agenciju i segment leta u datom vremenskom prozoru.
Rezultati se redom smeštaju u Cassandra tabele agencies_most_flight_segs i speed_stat_table. Pomoćne klase koje se koriste uz
glavnu klasu App.java su CassandraConnector, za povezivanje sa ovom bazom i FedsData, koja opisuje strukturu preuzete stavke skupa podataka. 

## Nadgledanje rada

Izlaz koji aplikacije i servisi štampaju u konzoli može se videti preko prozora Docker Tooling u okruženju Eclipse, nakon povezivanja
na aktivnu socket konekciju i izbora željenog kontejnera.
This repository contains example code and sample data for *Anatomy of Data Source API* session.
Follow the below steps to clone code and setup your machine.


## Prerequisites

* Java
* Maven 3


## 2. Getting code

           git clone https://github.com/phatak-dev/anatomy_of_spark_datasource_api


## 3. Build

        mvn clean install

### 4. Testing

then run the following command from code directory

     java -cp target/spark-datasource-examples.jar com.madhukaraphatak.spark.datasource.CsvSchemaDiscovery local src/main/resources/sales.csv


## 5. Loading into an IDE

You can run all the examples from terminal. If you want to run from the IDE, follow the below steps


* IDEA 14

 Install [scala](https://plugins.jetbrains.com/plugin/?id=1347) plugin. Once plugin is loaded you can load it as [maven
 project](https://www.jetbrains.com/idea/help/importing-project-from-maven-model.html).


## 6. Tags

This repository contains multiple tags to indicate progressive development of data source.
The following are the different tags and sequence of development

* v0.1 - Data source development starts from this. Schema discovery is implemented.
* v0.2 - Build scan is implemented
* v0.3 - Data type inference implemented
* v0.4 - Save option implemented
* v0.5 - Prune column implemented
* v0.6 - Prune filters implemented


## 7. Up to date

Please pull before coming to the session to get the latest code.
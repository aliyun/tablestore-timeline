# TableStore Timeline Lib V2


## Requirements
Java 1.6 or later

## Install
The recommended way to use the TableStore Timeline Lib in your project is to consume it from Maven. Import as follows:
```xml
<dependency>
    <groupId>com.aliyun.openservices.tablestore</groupId>
    <artifactId>Timeline</artifactId>
    <version>2.0.0</version>
</dependency>
```

## Build
Once you check out the code from GitHub, you can build it using Maven. Use the following command to build:
```shell
  mvn clean install -DskipTests
```

## New Feature
* Add timeline meta management.
* Add fuzzy search both meta and timeline by TableStore's SearchIndex.
* Support multi primaryKey column for Timeline Identifier.
* Support 2 kinds of sequence id, AUTO_INCREMENT and MANUAL.
* Compatible with Timeline 1.X, and [Click here for sample code](src/test/java/examples/v2/FitForTimelineV1.java).
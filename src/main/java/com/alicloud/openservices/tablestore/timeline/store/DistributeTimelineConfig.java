package com.alicloud.openservices.tablestore.timeline.store;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;
import com.alicloud.openservices.tablestore.timeline.message.StringMessage;
import com.alicloud.openservices.tablestore.writer.WriterConfig;

/**
 * 分布式Store系统的配置文件。
 * 这里的分布式Store使用阿里云的表格存储（Table Store）。
 */
public class DistributeTimelineConfig {
    /**
     * 阿里云Table Store的endpoint地址。
     */
    private String endpoint = null;

    /**
     * 阿里云访问秘钥的AccessKeyID。
     */
    private String accessKeyID = null;

    /**
     * 阿里云访问秘钥的AccessKeySecret。
     */
    private String accessKeySecret = null;

    /**
     * 阿里云Table Store实例的名称。
     */
    private String instanceName = null;

    /**
     * 阿里云Table Store中实例下的表名。存储系统和同步系统应该使用不同的表名。
     */
    private String tableName = null;

    /**
     * 阿里云Table Store中单次范围扫描最大返回个数。
     */
    private int limit = 100;

    /**
     * 阿里云TableStore中表的Time To Live，单位是秒。如果希望永久保存，则设置为-1即可。
     */
    private int ttl = -1;

    /**
     * 阿里云TableStore的客户端配置项，包括超时等。
     */
    private ClientConfiguration clientConfiguration = new ClientConfiguration();

    /**
     * 阿里云Table Store Writer的配置参数，适用于batch接口。
     */
    private WriterConfig writerConfig = new WriterConfig();

    /**
     * 表中第一个主键列的名字。
     */
    private String firstPKName = "timeline_id";

    /**
     * 表中第二个主键列的名字，这一列是自增列。
     */
    private String secondPKName = "sequence_id";

    /**
     * 消息构造类。
     */
    private IMessage messageInstance = new StringMessage();

    /**
     * 消息ID存储的属性列名称。
     */
    private String messageIDColumnNameSuffix = "message_id";

    /**
     * 消息内容列的前缀字符。
     */
    private String messageContentSuffix = "content";

    private String messageContentCountSuffix = "column_count";

    /**
     * 消息内容的crc32值，用于校验数据是否完整。
     */
    private String columnNameOfMessageCrc32Suffix = "crc32";

    /**
     * （Binary类型列）每一列的最大长度。默认1MB。
     */
    private int columnMaxLength = 1024 * 1024;


    /**
     * DistributeTimelineConfig的构造函数，构造函数里面的5个参数是必选项，其他参数都有默认值，属于可选项。
     * @param endpoint              Table Store中实例的endpoint。
     * @param accessKeyID           阿里云的Access Key ID。
     * @param accessKeySecret       阿里云Access Key Secret。
     * @param instanceName          Table Tore的实例名称。
     * @param tableName             表名。
     */
    public DistributeTimelineConfig(String endpoint, String accessKeyID, String accessKeySecret,
                             String instanceName, String tableName)
    {
        this.endpoint = endpoint;
        this.accessKeyID = accessKeyID;
        this.accessKeySecret = accessKeySecret;
        this.instanceName = instanceName;
        this.tableName = tableName;
    }

    /**
     * 获取endpoint。
     * @return  endpoint。
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * 设置TableStore Endpoint。
     * @param endpoint endpoint。
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * 获取阿里云access key id。
     * @return  AccessKeyID。
     */
    public String getAccessKeyID() {
        return accessKeyID;
    }

    /**
     * 设置阿里云access key id。
     * @param accessKeyID AccessKeyID。
     */
    public void setAccessKeyID(String accessKeyID) {
        this.accessKeyID = accessKeyID;
    }

    /**
     * 获取阿里云access key secret。
     * @return  AccessKeySecret。
     */
    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    /**
     * 设置阿里云access key secret。
     * @param accessKeySecret   AccessKeySecret。
     */
    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }

    /**
     * 获取实例名称。
     * @return  实例名称。
     */
    public String getInstanceName() {
        return instanceName;
    }

    /**
     * 设置实例名称。
     * @param instanceName  实例名称。
     */
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    /**
     * 获取表名。
     * @return  表名。
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置表名。
     * @param tableName 表名。
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 返回每次Scan操作的最大返回个数。
     * @return  最大返回个数。
     */
    public int getLimit() {
        return limit;
    }

    /**
     * 设置每次Scan操作的最大返回个数。
     * @param limit 最大返回个数。
     */
    public void setLimit(int limit) {
        this.limit = limit;
    }

    /**
     * 获取表的TTL（Time-To-Live）。
     * @return  表的TTL。
     */
    public int getTtl() {
        return ttl;
    }

    /**
     * 设置表的TTL（Time-To-Live）。
     * @param ttl   表的TTL。
     */
    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    /**
     * 获取TableStore SDK的ClientConfiguration。
     * @return  TableStore SDK的ClientConfiguration。
     */
    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    /**
     * 设置TableStore SDK的ClientConfiguration。
     * @param clientConfiguration TableStore SDK的ClientConfiguration。
     */
    public void setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
    }

    /**
     * 获取第一列PK的名称。
     * @return  第一列名称的名称。
     */
    public String getFirstPKName() {
        return firstPKName;
    }

    /**
     * 设置第一列PK的名称。
     * 第一列用来存储TimelineID，用来唯一标识一个Timeline。
     * 如果某些场景下，需要多列才能唯一标识一个Timeline，此时可以将多列合并为一列。
     * @param firstPKName   第一列名称的名称。
     */
    public void setFirstPKName(String firstPKName) {
        this.firstPKName = firstPKName;
    }

    /**
     * 获取第二列PK的名称。
     * @return 第二列名称的名称。
     */
    public String getSecondPKName() {
        return secondPKName;
    }

    /**
     * 设置第二列PK的名称。
     * 第二列用来存储顺序ID（sequenceID），这一列会采用主键列自增功能。
     * @param secondPKName 第二列PK的名称。
     */
    public void setSecondPKName(String secondPKName) {
        this.secondPKName = secondPKName;
    }

    /**
     * 获取消息类型的实例，用来在读取到消息的时候构造同类型消息。
     * @return  消息实例。
     */
    public IMessage getMessageInstance() {
        return messageInstance;
    }

    /**
     * 设置消息类型的实例，用来在读取到消息的时候构造同类型消息。
     * @param messageInstance 消息实例。
     */
    public void setMessageInstance(IMessage messageInstance) {
        this.messageInstance = messageInstance;
    }

    /**
     * 获取消息ID所在列的列名。
     * @return  列名。
     */
    public String getMessageIDColumnNameSuffix() {
        return messageIDColumnNameSuffix;
    }

    /**
     * 设置消息ID所在列的列名后缀。
     * @param messageIDColumnNameSuffix 消息ID所在列的列名后缀。
     */
    public void setMessageIDColumnNameSuffix(String messageIDColumnNameSuffix) {
        this.messageIDColumnNameSuffix = messageIDColumnNameSuffix;
    }

    /**
     * 获取消息内容列的名称前缀。
     * @return  列名前缀。
     */
    public String getMessageContentSuffix() {
        return messageContentSuffix;
    }

    /**
     * 设置消息内容列的名称前缀。
     * @param messageContentSuffix 消息内容列的名称后缀。
     */
    public void setMessageContentSuffix(String messageContentSuffix) {
        this.messageContentSuffix = messageContentSuffix;
    }

    /**
     * 获取存储消息内容的CRC32结果的列名后缀。
     * @return  存储CRC32的列名后缀。
     */
    public String getColumnNameOfMessageCrc32Suffix() {
        return columnNameOfMessageCrc32Suffix;
    }

    /**
     * 设置存储消息内容的CRC32结果的列名后缀。
     * @param columnNameOfMessageCrc32Suffix 存储CRC32的列名后缀。
     */
    public void setColumnNameOfMessageCrc32Suffix(String columnNameOfMessageCrc32Suffix) {
        this.columnNameOfMessageCrc32Suffix = columnNameOfMessageCrc32Suffix;
    }

    /**
     * 获取每一列的最大长度。
     * @return  列长度最大值。
     */
    public int getColumnMaxLength() {
        return columnMaxLength;
    }

    /**
     * 设置每一列的最大长度。
     * @param columnMaxLength   列长度最大值。
     */
    public void setColumnMaxLength(int columnMaxLength) {
        this.columnMaxLength = columnMaxLength;
    }

    /**
     * 获取WriterConfig配置。
     * @return  WriterConfig配置。
     */
    public WriterConfig getWriterConfig() {
        return writerConfig;
    }

    /**
     * 设置适用于Batch接口的WriterConfig配置。
     * @param writerConfig WriterConfig配置。
     */
    public void setWriterConfig(WriterConfig writerConfig) {
        this.writerConfig = writerConfig;
    }

    /**
     * 获取消息内容列的个数，主要是为了避免update内容后，内容列变短带来的影响。
     * @return  消息内容列个数的后缀
     */
    public String getMessageContentCountSuffix() {
        return messageContentCountSuffix;
    }

    /**
     * 设置消息内容列的个数，主要是为了避免update内容后，内容列变短带来的影响。
     * @param messageContentCountSuffix     消息内容列个数的后缀
     */
    public void setMessageContentCountSuffix(String messageContentCountSuffix) {
        this.messageContentCountSuffix = messageContentCountSuffix;
    }
}

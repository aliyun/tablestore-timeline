package com.alicloud.openservices.tablestore.timeline2.common;

import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;

public class Conf {
    private String endpoint;
    private String accessId;
    private String accessKey;
    private String instanceName;

    private Conf() {}

    public static Conf newInstance(String confPath) throws FileNotFoundException {
        Reader reader = new FileReader(confPath);
        Gson gson = new Gson();
        Conf f = gson.fromJson(reader, Conf.class);
        return f;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }
}

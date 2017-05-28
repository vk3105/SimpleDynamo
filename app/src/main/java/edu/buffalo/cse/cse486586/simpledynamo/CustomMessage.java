package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A class for handling message communications
 * Created by vipin on 4/9/17.
 */

public class CustomMessage {

    private int senderPort;
    private int toPort;
    private String messageType;
    private String key;
    private String value;
    private String timeStamp;

    private ConcurrentHashMap<String, String[]> keyValMap;

    public CustomMessage(String messageType, int senderPort, int toPort, String key, String value,
                         String timeStamp, ConcurrentHashMap<String, String[]> keyValMap) {
        this.key = key;
        this.value = value;
        this.toPort = toPort;
        this.keyValMap = keyValMap;
        this.timeStamp = timeStamp;
        this.senderPort = senderPort;
        this.messageType = messageType;
    }

    private String getKeyValuePair() {
        String keyValPairs = "";
        for (String key : keyValMap.keySet()) {
            String value[] = keyValMap.get(key);
            keyValPairs += (key + Constants.DATADELIM + value[0] + "," + value[1] + "," + value[2] + Constants.DATASEPDELIM);
        }
        keyValPairs = keyValPairs.substring(0, keyValPairs.lastIndexOf(Constants.DATASEPDELIM));
        return keyValPairs;
    }

    @Override
    public String toString() {
        String customMessageStr = "";

        customMessageStr = messageType + Constants.DELIM +
                senderPort + Constants.DELIM +
                toPort + Constants.DELIM +
                key + Constants.DELIM +
                value + Constants.DELIM +
                timeStamp;
        if (keyValMap != null && keyValMap.size() != 0) {
            customMessageStr += Constants.DELIM +
                    getKeyValuePair();
        } else {
            customMessageStr += Constants.DELIM + Constants.NULLVALUE;
        }

        return customMessageStr;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getSenderPort() {
        return senderPort;
    }

    public void setSenderPort(int senderPort) {
        this.senderPort = senderPort;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public ConcurrentHashMap<String, String[]> getKeyValMap() {
        return keyValMap;
    }

    public void setKeyValMap(ConcurrentHashMap<String, String[]> keyValMap) {
        this.keyValMap = keyValMap;
    }

    public int getToPort() {
        return toPort;
    }

    public void setToPort(int toPort) {
        this.toPort = toPort;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

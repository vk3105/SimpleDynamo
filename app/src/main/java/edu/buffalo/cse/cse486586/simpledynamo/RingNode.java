package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by vipin on 4/10/17.
 */


public class RingNode implements Comparable<RingNode> {

    private String myKeyHash;
    private String mySuccKeyHash;
    private String myPredKeyHash;

    private int myPort;
    private int mySuccPort;
    private int myPredPort;
    private int mySecondSuccPort;
    private int mySecondPredPort;

    private static String RING_DELIM = "\n";

    public RingNode() {

    }

    public RingNode(int myPort, int succPort, int mySecondSuccPort, int predPort, int mySecondPredPort, String myKeyHash, String mySuccKeyHash,
                    String myPredKeyHash) {
        this.myPort = myPort;
        this.mySuccPort = succPort;
        this.mySecondSuccPort = mySecondSuccPort;
        this.myPredPort = predPort;
        this.mySecondPredPort = mySecondPredPort;
        this.myKeyHash = myKeyHash;
        this.mySuccKeyHash = mySuccKeyHash;
        this.myPredKeyHash = myPredKeyHash;
    }

    public String getMyKeyHash() {
        return myKeyHash;
    }

    public int getMySuccPort() {
        return mySuccPort;
    }

    public int getMyPredPort() {
        return myPredPort;
    }

    public int getMySecondSuccPort() {
        return mySecondSuccPort;
    }

    public void setMySecondSuccPort(int mySecondSuccPort) {
        this.mySecondSuccPort = mySecondSuccPort;
    }

    public String getMySuccKeyHash() {
        return mySuccKeyHash;
    }

    public void setMySuccKeyHash(String mySuccKeyHash) {
        this.mySuccKeyHash = mySuccKeyHash;
    }

    public void setMyPredKeyHash(String myPredKeyHash) {
        this.myPredKeyHash = myPredKeyHash;
    }

    public int getMyPort() {
        return myPort;
    }

    public int getSuccPort() {
        return mySuccPort;
    }

    public void setSuccPort(int succPort) {
        this.mySuccPort = succPort;
    }

    public int getPredPort() {
        return myPredPort;
    }

    public void setPredPort(int predPort) {
        this.myPredPort = predPort;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof RingNode)) {
            return false;
        }

        RingNode secondNode = (RingNode) object;

        if (this.myKeyHash.equals(secondNode.getMyKeyHash())) {
            return true;
        }
        return false;
    }

    public int getMySecondPredPort() {
        return mySecondPredPort;
    }

    public void setMySecondPredPort(int mySecondPredPort) {
        this.mySecondPredPort = mySecondPredPort;
    }

    @Override
    public int compareTo(RingNode obj) {
        return this.myKeyHash.compareTo(obj.getMyKeyHash());
    }

    @Override
    public String toString() {
        String str = myKeyHash + RING_DELIM + mySuccKeyHash + RING_DELIM + myPredKeyHash + RING_DELIM + myPort + RING_DELIM + mySuccPort
                + RING_DELIM + mySecondSuccPort + RING_DELIM + myPredPort + RING_DELIM + mySecondPredPort;
        return str;
    }

}
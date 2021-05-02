package com.sliit.dc;

import java.io.Serializable;

public class ReplicateLocation implements Serializable {

    private static final long serialVersionUID = -4113307750760738108L;

    private String address;
    private int id;
    private boolean isAlive;

    public ReplicateLocation(String address, int id, boolean isAlive) {
        this.address = address;
        this.id = id;
        this.isAlive = isAlive;
    }

    boolean isAlive(){
        return isAlive;
    }

    int getId(){
        return id;
    }

    void setAlive(boolean isAlive){
        this.isAlive = isAlive;
    }

    String getAddress(){
        return address;
    }
}

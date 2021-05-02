package com.sliit.dc;

import java.io.Serializable;

public class WriteAck implements Serializable {

    private static final long serialVersionUID = -4764830257785399352L;

    private long transactionId;
    private long timeStamp;
    private ReplicateLocation loction;

    public WriteAck(long transactionId, long timeStamp, ReplicateLocation loction) {
        this.transactionId = transactionId;
        this.timeStamp = timeStamp;
        this.loction = loction;
    }

    public long getTransactionId() {
        return transactionId;
    }


    public long getTimeStamp() {
        return timeStamp;
    }


    public ReplicateLocation getLoc() {
        return loction;
    }
}

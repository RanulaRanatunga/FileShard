package com.sliit.dc;

import java.io.Serializable;

public class ChunkSector implements Serializable {

    private static final long serialVersionUID = 4267009886985001938L;

    private long transactionId;
    private long sequenceNumber;


    public ChunkSector(long tid, long sequenceNumber) {
        this.transactionId = tid;
        this.sequenceNumber = sequenceNumber;
    }

    public long getSeqNo(){
        return sequenceNumber;
    }

    public long getTxnID() {
        return transactionId;
    }
}

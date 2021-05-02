package com.sliit.dc;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

public class Client {

    MasterServerClientInterface masterStub;
    static Registry registry;
    int regPort = Configs.REG_PORT;
    String regAddr = Configs.REG_ADDR;
    int chunkSize = Configs.CHUNK_SIZE;

    public Client() {
        try {
            registry = LocateRegistry.getRegistry(regAddr, regPort);
            masterStub =  (MasterServerClientInterface) registry.lookup("MasterServerClientInterface");
            System.out.println("[@client] Master Stub fetched successfuly");
        } catch (RemoteException | NotBoundException e) {

            e.printStackTrace();
        }
    }

    public byte[] read(String fileName) throws IOException, NotBoundException{
        List<ReplicateLocation> locations = masterStub.read(fileName);
        System.out.println("[@client] Master Granted read operation");

        // TODO fetch from all and verify
        ReplicateLocation replicaLoc = locations.get(0);

        ReplicateServerClientInterface replicaStub = (ReplicateServerClientInterface) registry.lookup("ReplicateClient"+replicaLoc.getId());
        FileContent fileContent = replicaStub.read(fileName);
        System.out.println("[@client] read operation completed successfuly");
        System.out.println("[@client] data:");

        System.out.println(new String(fileContent.getData()));
        return fileContent.getData();
    }

    public ReplicateServerClientInterface initWrite(String fileName, Long txnID) throws IOException, NotBoundException{
        WriteAck ackMsg = masterStub.write(fileName);
        txnID = new Long(ackMsg.getTransactionId());
        return (ReplicateServerClientInterface) registry.lookup("ReplicaClient"+ackMsg.getLoc().getId());
    }

    public void writeChunk (long txnID, String fileName, byte[] chunk, long seqN, ReplicateServerClientInterface replicaStub) throws RemoteException, IOException{

        FileContent fileContent = new FileContent(fileName, chunk);
        ChunkSector chunkSector;

        do {
            chunkSector = replicaStub.write(txnID, seqN, fileContent);
        } while(chunkSector.getSeqNo() != seqN);
    }

    public void write (String fileName, byte[] data) throws IOException, NotBoundException, MessageNotFoundException{
        WriteAck ackMsg = masterStub.write(fileName);
        ReplicateServerClientInterface replicaStub = (ReplicateServerClientInterface) registry.lookup("ReplicateClient"+ackMsg.getLoc().getId());

        System.out.println("[@client] Master granted write operation");

        int segN = (int) Math.ceil(1.0*data.length/chunkSize);
        FileContent fileContent = new FileContent(fileName);
        ChunkSector chunkSector;
        byte[] chunk = new byte[chunkSize];

        for (int i = 0; i < segN-1; i++) {
            System.arraycopy(data, i*chunkSize, chunk, 0, chunkSize);
            fileContent.setData(chunk);
            do {
                chunkSector = replicaStub.write(ackMsg.getTransactionId(), i, fileContent);
            } while(chunkSector.getSeqNo() != i);
        }

        int lastChunkLen = chunkSize;
        if (data.length%chunkSize > 0)
            lastChunkLen = data.length%chunkSize;
        chunk = new byte[lastChunkLen];
        System.arraycopy(data, segN-1, chunk, 0, lastChunkLen);
        fileContent.setData(chunk);
        do {
            chunkSector = replicaStub.write(ackMsg.getTransactionId(), segN-1, fileContent);
        } while(chunkSector.getSeqNo() != segN-1 );


        System.out.println("[@client] write operation complete");
        replicaStub.commit(ackMsg.getTransactionId(), segN);
        System.out.println("[@client] commit operation complete");
    }

    public void commit(String fileName, long txnID, long seqN) throws MessageNotFoundException, IOException, NotBoundException{
        ReplicateLocation primaryLoc = masterStub.locatePrimaryReplica(fileName);
        ReplicateServerClientInterface primaryStub = (ReplicateServerClientInterface) registry.lookup("ReplicaClient"+primaryLoc.getId());
        primaryStub.commit(txnID, seqN);
        System.out.println("[@client] commit operation complete");
    }

    public void batchOperations(String[] cmds){
        System.out.println("[@client] batch operations started");
        String cmd ;
        String[] tokens;
        for (int i = 0; i < cmds.length; i++) {
            cmd = cmds[i];
            tokens = cmd.split(", ");
            try {
                if (tokens[0].trim().equals("read"))
                    this.read(tokens[1].trim());
                else if (tokens[0].trim().equals("write"))
                    this.write(tokens[1].trim(), tokens[2].trim().getBytes());
                else if (tokens[0].trim().equals("commit"))
                    this.commit(tokens[1].trim(), Long.parseLong(tokens[2].trim()), Long.parseLong(tokens[3].trim()));
            }catch (IOException | NotBoundException | MessageNotFoundException e){
                System.err.println("Operation "+i+" Failed");
            }
        }
        System.out.println("[@client] batch operations completed");
    }


}

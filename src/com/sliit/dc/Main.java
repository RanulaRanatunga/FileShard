package com.sliit.dc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

public class Main {

    static int regPort = Configs.REG_PORT;

    static Registry registry ;

    static void respawnReplicaServers(Master master)throws IOException {
        System.out.println("[@main] Find replica servers ");

        BufferedReader br = new BufferedReader(new FileReader("Servers.txt"));
        int n = Integer.parseInt(br.readLine().trim());
        ReplicateLocation replicateLocation;
        String s;

        for (int i = 0; i < n; i++) {
            s = br.readLine().trim();
            replicateLocation = new ReplicateLocation(s.substring(0, s.indexOf(':')), i, true);
            ReplicateServer rs = new ReplicateServer(i, "./");

            ReplicateInterface stub = (ReplicateInterface) UnicastRemoteObject.exportObject(rs, 0);
            registry.rebind("ReplicaClient"+i, stub);

            master.registerReplicaServer(replicateLocation, stub);

            System.out.println("replica server state [@ main] = "+rs.isAlive());
        }
        br.close();
    }

    public static void launchClients(){
        try {
            Client c = new Client();
            char[] ss = "File 1 test test END ".toCharArray();
            byte[] data = new byte[ss.length];
            for (int i = 0; i < ss.length; i++)
                data[i] = (byte) ss[i];

            c.write("file1", data);
            byte[] ret = c.read("file1");
            System.out.println("file1: " + ret);

            c = new Client();
            ss = "File 1 Again Again END ".toCharArray();
            data = new byte[ss.length];
            for (int i = 0; i < ss.length; i++)
                data[i] = (byte) ss[i];

            c.write("file1", data);
            ret = c.read("file1");
            System.out.println("file1: " + ret);

            c = new Client();
            ss = "File 2 test test END ".toCharArray();
            data = new byte[ss.length];
            for (int i = 0; i < ss.length; i++)
                data[i] = (byte) ss[i];

            c.write("file2", data);
            ret = c.read("file2");
            System.out.println("file2: " + ret);

        } catch (NotBoundException | IOException | MessageNotFoundException e) {
            e.printStackTrace();
        }
    }

    public  static void customTest() throws IOException, NotBoundException, MessageNotFoundException{
        Client c = new Client();
        String fileName = "file1";

        char[] ss = "[INITIAL DATA!]".toCharArray();
        byte[] data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++)
            data[i] = (byte) ss[i];

        c.write(fileName, data);

        c = new Client();
        ss = "File 1 test test END".toCharArray();
        data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++)
            data[i] = (byte) ss[i];


        byte[] chunk = new byte[Configs.CHUNK_SIZE];

        int seqN =data.length/Configs.CHUNK_SIZE;
        int lastChunkLen = Configs.CHUNK_SIZE;

        if (data.length%Configs.CHUNK_SIZE > 0) {
            lastChunkLen = data.length%Configs.CHUNK_SIZE;
            seqN++;
        }

        WriteAck ackMsg = c.masterStub.write(fileName);
        ReplicateServerClientInterface stub = (ReplicateServerClientInterface) registry.lookup("ReplicaClient"+ackMsg.getLoc().getId());

        FileContent fileContent;
        @SuppressWarnings("unused")
        ChunkSector chunkAck;
        //		for (int i = 0; i < seqN; i++) {
        System.arraycopy(data, 0*Configs.CHUNK_SIZE, chunk, 0, Configs.CHUNK_SIZE);
        fileContent = new FileContent(fileName, chunk);
        chunkAck = stub.write(ackMsg.getTransactionId(), 0, fileContent);


        System.arraycopy(data, 1*Configs.CHUNK_SIZE, chunk, 0, Configs.CHUNK_SIZE);
        fileContent = new FileContent(fileName, chunk);
        chunkAck = stub.write(ackMsg.getTransactionId(), 1, fileContent);

        // read here
        List<ReplicateLocation> locations = c.masterStub.read(fileName);
        System.err.println("[@CustomTest] Read1 started ");

        ReplicateLocation replicaLoc = locations.get(0);
        ReplicateServerClientInterface replicaStub = (ReplicateServerClientInterface) registry.lookup("ReplicateClient"+replicaLoc.getId());
        fileContent = replicaStub.read(fileName);
        System.err.println("[@CustomTest] data:");
        System.err.println(new String(fileContent.getData()));

        for(int i = 2; i < seqN-1; i++){
            System.arraycopy(data, i*Configs.CHUNK_SIZE, chunk, 0, Configs.CHUNK_SIZE);
            fileContent = new FileContent(fileName, chunk);
            chunkAck = stub.write(ackMsg.getTransactionId(), i, fileContent);
        }
        // copy the last chuck that might be < CHUNK_SIZE
        System.arraycopy(data, (seqN-1)*Configs.CHUNK_SIZE, chunk, 0, lastChunkLen);
        fileContent = new FileContent(fileName, chunk);
        chunkAck = stub.write(ackMsg.getTransactionId(), seqN-1, fileContent);


        ReplicateLocation primaryLoc = c.masterStub.locatePrimaryReplica(fileName);
        ReplicateServerClientInterface primaryStub = (ReplicateServerClientInterface) registry.lookup("ReplicaClient"+primaryLoc.getId());
        primaryStub.commit(ackMsg.getTransactionId(), seqN);

        locations = c.masterStub.read(fileName);
        System.err.println("[@CustomTest] Read3 started ");

        replicaLoc = locations.get(0);
        replicaStub = (ReplicateServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
        fileContent = replicaStub.read(fileName);
        System.err.println("[@CustomTest] data:");
        System.err.println(new String(fileContent.getData()));


        for(int i = 2; i < seqN-1; i++){
            System.arraycopy(data, i*Configs.CHUNK_SIZE, chunk, 0, Configs.CHUNK_SIZE);
            fileContent = new FileContent(fileName, chunk);
            chunkAck = stub.write(ackMsg.getTransactionId(), i, fileContent);
        }

        System.arraycopy(data, (seqN-1)*Configs.CHUNK_SIZE, chunk, 0, lastChunkLen);
        fileContent = new FileContent(fileName, chunk);
        chunkAck = stub.write(ackMsg.getTransactionId(), seqN-1, fileContent);

    }

    static Master startMaster() throws AccessException, RemoteException {
        Master master = new Master();
        MasterServerClientInterface stub =
                (MasterServerClientInterface) UnicastRemoteObject.exportObject(master, 0);
        registry.rebind("MasterServerClientInterface", (Remote) stub);
        System.err.println("Server ready");
        return master;
    }

    public static void main(String[] args) throws IOException {


        try {
            LocateRegistry.createRegistry(regPort);
            registry = LocateRegistry.getRegistry(regPort);

            Master master = startMaster();
            respawnReplicaServers(master);
            launchClients();

        } catch (RemoteException   e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }
}

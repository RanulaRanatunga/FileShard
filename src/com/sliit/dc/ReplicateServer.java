package com.sliit.dc;

import java.io.*;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplicateServer implements ReplicateServerClientInterface, ReplicateMasterInterface, ReplicateReplicaInterface, Remote {


    private int regPort = Configs.REG_PORT;
    private String regAddr = Configs.REG_ADDR;

    private int id;
    private String dir;
    private Registry registry;

    private Map<Long, String> activeTxn;
    private Map<Long, Map<Long, byte[]>> txnFileMap;
    private Map<String, List<ReplicateReplicaInterface>> filesReplicaMap;
    private Map<Integer, ReplicateLocation> replicaServersLoc;
    private Map<Integer, ReplicateReplicaInterface> replicaServersStubs;
    private ConcurrentMap<String, ReentrantReadWriteLock> locks;

    public ReplicateServer(int id, String dir) {
        this.id = id;
        this.dir = dir+"/Replica_"+id+"/";
        txnFileMap = new TreeMap<Long, Map<Long, byte[]>>();
        activeTxn = new TreeMap<Long, String>();
        filesReplicaMap = new TreeMap<String, List<ReplicateReplicaInterface>>();
        replicaServersLoc = new TreeMap<Integer, ReplicateLocation>();
        replicaServersStubs = new TreeMap<Integer, ReplicateReplicaInterface>();
        locks = new ConcurrentHashMap<String, ReentrantReadWriteLock>();

        File file = new File(this.dir);
        if (!file.exists()){
            file.mkdir();
        }

        try  {
            registry = LocateRegistry.getRegistry(regAddr, regPort);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createFile(String fileName) throws IOException {
        File file = new File(dir+fileName);

        locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
        ReentrantReadWriteLock lock = locks.get(fileName);

        lock.writeLock().lock();
        file.createNewFile();
        lock.writeLock().unlock();
    }

    @Override
    public FileContent read(String fileName) throws FileNotFoundException,
            RemoteException, IOException {
        File f = new File(dir+fileName);

        locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
        ReentrantReadWriteLock lock = locks.get(fileName);

        @SuppressWarnings("resource")
        BufferedInputStream br = new BufferedInputStream(new FileInputStream(f));

        // assuming files are small and can fit in memory
        byte data[] = new byte[(int) (f.length())];

        lock.readLock().lock();
        br.read(data);
        lock.readLock().unlock();

        FileContent content = new FileContent(fileName, data);
        return content;
    }

    @Override
    public ChunkSector write(long txnID, long msgSeqNum, FileContent data)
            throws RemoteException, IOException {
        System.out.println("[@ReplicaServer] write "+msgSeqNum);

        if (!txnFileMap.containsKey(txnID)){
            txnFileMap.put(txnID, new TreeMap<Long, byte[]>());
            activeTxn.put(txnID, data.getFileName());
        }

        Map<Long, byte[]> chunkMap =  txnFileMap.get(txnID);
        chunkMap.put(msgSeqNum, data.getData());
        return new ChunkSector(txnID, msgSeqNum);
    }

    @Override
    public boolean commit(long txnID, long numOfMsgs)
            throws MessageNotFoundException, RemoteException, IOException {


        System.out.println("[@Replica] commit intiated");
        Map<Long, byte[]> chunkMap = txnFileMap.get(txnID);
        if (chunkMap.size() < numOfMsgs)
            throw new MessageNotFoundException();

        String fileName = activeTxn.get(txnID);
        List<ReplicateReplicaInterface> slaveReplicas = filesReplicaMap.get(fileName);

        for (ReplicateReplicaInterface replica : slaveReplicas) {
            boolean sucess = replica.reflectUpdate(txnID, fileName, new ArrayList<>(chunkMap.values()));
            if (!sucess) {
                // TODO handle failure
            }
        }


        BufferedOutputStream bw =new BufferedOutputStream(new FileOutputStream(dir+fileName, true));

        locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
        ReentrantReadWriteLock lock = locks.get(fileName);

        lock.writeLock().lock();
        for (Iterator<byte[]> iterator = chunkMap.values().iterator(); iterator.hasNext();)
            bw.write(iterator.next());
        bw.close();
        lock.writeLock().unlock();


        for (ReplicateReplicaInterface replica : slaveReplicas)
            replica.releaseLock(fileName);


        activeTxn.remove(txnID);
        txnFileMap.remove(txnID);

        return false;
    }

    @Override
    public boolean abort(long txnID) throws RemoteException {
        activeTxn.remove(txnID);
        filesReplicaMap.remove(txnID);
        return false;
    }


    @Override
    public boolean reflectUpdate(long txnID, String fileName, ArrayList<byte[]> data) throws IOException{
        System.out.println("[@Replica] reflect update initiated");
        BufferedOutputStream bw =new BufferedOutputStream(new FileOutputStream(dir+fileName, true));


        locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
        ReentrantReadWriteLock lock = locks.get(fileName);

        lock.writeLock().lock(); // don't release lock here .. making sure coming reads can't proceed
        for (Iterator<byte[]> iterator = data.iterator(); iterator.hasNext();)
            bw.write(iterator.next());
        bw.close();


        activeTxn.remove(txnID);
        return true;
    }

    @Override
    public void releaseLock(String fileName) {
        ReentrantReadWriteLock lock = locks.get(fileName);
        lock.writeLock().unlock();
    }

    @Override
    public void takeCharge(String fileName, List<ReplicateLocation> slaveReplicas) throws AccessException, RemoteException, NotBoundException {
        System.out.println("[@Replica] taking charge of file: "+fileName);
        System.out.println(slaveReplicas);

        List<ReplicateReplicaInterface> slaveReplicasStubs = new ArrayList<ReplicateReplicaInterface>(slaveReplicas.size());

        for (ReplicateLocation loc : slaveReplicas) {
            // if the current locations is this replica .. ignore
            if (loc.getId() == this.id)
                continue;

            if (!replicaServersLoc.containsKey(loc.getId())){
                replicaServersLoc.put(loc.getId(), loc);
                ReplicateReplicaInterface stub = (ReplicateReplicaInterface) registry.lookup("ReplicaClient"+loc.getId());
                replicaServersStubs.put(loc.getId(), stub);
            }
            ReplicateReplicaInterface replicaStub = replicaServersStubs.get(loc.getId());
            slaveReplicasStubs.add(replicaStub);
        }

        filesReplicaMap.put(fileName, slaveReplicasStubs);
    }


    @Override
    public boolean isAlive() {
        return true;
    }


}

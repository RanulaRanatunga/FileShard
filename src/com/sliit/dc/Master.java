package com.sliit.dc;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;

public class Master implements MasterReplicaInterface, MasterServerClientInterface, Remote {

    class HeartBeatTask extends TimerTask {

        @Override
        public void run() {
            for (ReplicateLocation replicaLoc : replicaServersLocs) {
                try {
                    replicaServersStubs.get(replicaLoc.getId()).isAlive();
                } catch (RemoteException e) {
                    replicaLoc.setAlive(false);
                    e.printStackTrace();
                }
            }
        }
    }

    private int nextTID;
    private int heartBeatRate = Configs.HEART_BEAT_RATE;
    private int replicationN = Configs.REPLICATION_N;
    private Timer HeartBeatTimer;
    private Random randomGen;

    private Map<String,	 List<ReplicateLocation> > filesLocationMap;
    private Map<String,	 ReplicateLocation> primaryReplicaMap;
    private List<ReplicateLocation> replicaServersLocs;
    private List<ReplicateMasterInterface> replicaServersStubs;

    public Master() {
        filesLocationMap = new HashMap<String, List<ReplicateLocation>>();
        primaryReplicaMap = new HashMap<String, ReplicateLocation>();
        replicaServersLocs = new ArrayList<ReplicateLocation>();
        replicaServersStubs = new ArrayList<ReplicateMasterInterface>();

        nextTID = 0;
        randomGen = new Random();

        HeartBeatTimer = new Timer();
        HeartBeatTimer.scheduleAtFixedRate(new HeartBeatTask(), 0, heartBeatRate);
    }

    /**
     * elects a new primary replica for the given file
     * @param fileName
     */

    private void assignNewMaster(String fileName){
        List<ReplicateLocation> replicas = filesLocationMap.get(fileName);
        boolean newPrimaryAssigned = false;
        for (ReplicateLocation replicaLoc : replicas) {
            if (replicaLoc.isAlive()){
                newPrimaryAssigned = true;
                primaryReplicaMap.put(fileName, replicaLoc);
                try {
                    replicaServersStubs.get(replicaLoc.getId()).takeCharge(fileName, filesLocationMap.get(fileName));
                } catch (RemoteException | NotBoundException e) {
                    e.printStackTrace();
                }
                break;
            }
        }

        if (!newPrimaryAssigned){
            //TODO a7a ya3ni
        }
    }

    /**
     * creates a new file @ N replica servers that are randomly chosen
     * elect the primary replica at random
     * @param fileName
     */

    private void createNewFile(String fileName){
        System.out.println("[@Master] Creating new file initiated");
        int luckyReplicas[] = new int[replicationN];
        List<ReplicateLocation> replicas = new ArrayList<ReplicateLocation>();

        Set<Integer> chosenReplicas = new TreeSet<Integer>();

        for (int i = 0; i < luckyReplicas.length; i++) {

            // TODO if no replica alive enter infinte loop
            do {
                luckyReplicas[i] = randomGen.nextInt(replicationN);

            } while(!replicaServersLocs.get(luckyReplicas[i]).isAlive() || chosenReplicas.contains(luckyReplicas[i]));


            chosenReplicas.add(luckyReplicas[i]);
            // add the lucky replica to the list of replicas maintaining the file
            replicas.add(replicaServersLocs.get(luckyReplicas[i]));

            // create the file at the lucky replicas
            try {
                replicaServersStubs.get(luckyReplicas[i]).createFile(fileName);
            } catch (IOException e) {
                // failed to create the file at replica server
                e.printStackTrace();
            }

        }

        // the primary replica is the first lucky replica picked
        int primary = luckyReplicas[0];
        try {
            replicaServersStubs.get(primary).takeCharge(fileName, replicas);
        } catch (RemoteException | NotBoundException e) {

            e.printStackTrace();
        }

        filesLocationMap.put(fileName, replicas);
        primaryReplicaMap.put(fileName, replicaServersLocs.get(primary));

    }

    @Override
    public List<ReplicateLocation> read(String fileName) throws FileNotFoundException,
            IOException, RemoteException {
        List<ReplicateLocation> replicateLocation = filesLocationMap.get(fileName);
        if (replicateLocation == null)
            throw new FileNotFoundException();
        return replicateLocation;
    }

    @Override
    public WriteAck write(String fileName) throws RemoteException, IOException {
        System.out.println("[@Master] write request initiated");
        long timeStamp = System.currentTimeMillis();

        List<ReplicateLocation> replicaLocs= filesLocationMap.get(fileName);
        int tid = nextTID++;
        if (replicaLocs == null)
            createNewFile(fileName);

        ReplicateLocation primaryReplicaLoc = primaryReplicaMap.get(fileName);

        if (primaryReplicaLoc == null)
            throw new IllegalStateException("No primary replica found");

        if (!primaryReplicaLoc.isAlive()){
            assignNewMaster(fileName);
            primaryReplicaLoc = primaryReplicaMap.get(fileName);
        }

        return new WriteAck(tid, timeStamp,primaryReplicaLoc);
    }

    @Override
    public ReplicateLocation locatePrimaryReplica(String fileName)
            throws RemoteException {

        return primaryReplicaMap.get(fileName);
    }

    /**
     * registers new replica server @ the master by adding required meta data
     * @param replicaLoc
     * @param replicaStub
     */
    public void registerReplicaServer(ReplicateLocation replicaLoc, ReplicateInterface replicaStub){
        replicaServersLocs.add(replicaLoc);
        replicaServersStubs.add( (ReplicateMasterInterface) replicaStub);
    }


}

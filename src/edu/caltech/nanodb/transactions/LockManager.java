package edu.caltech.nanodb.transactions;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;


/**
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 3/2/12
 * Time: 3:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class LockManager {

    private static class LockRequest {
        /** The owner requesting the lock. */
        int ownerID;

        /** The requested lock mode. */
        LockMode mode;

        public LockRequest(int ownerID, LockMode mode) {
            this.ownerID = ownerID;
            this.mode = mode;
        }
    }


    private static class LockedEntity {
        /** The entity that was locked. */
        Object entity;

        /** The IDs of owners that currently hold the entity. */
        HashSet<Integer> owners;

        /**
         * The mode of the lock held on the entity.  If there are multiple
         * owners, they all have the same kind of lock.
         */
        LockMode currentMode;

        /** These are lock requests that are currently waiting on the entity. */
        LinkedList<LockRequest> waitingRequests = new LinkedList<LockRequest>();

        
        public LockedEntity(Object entity, int initialOwnerID,
                            LockMode initialMode) {
            this.entity = entity;

            owners = new HashSet<Integer>();
            owners.add(initialOwnerID);

            currentMode = initialMode;
        }
        

        public void request(int ownerID, LockMode mode) {
            if (currentMode == LockMode.SHARED) {
                if (mode == LockMode.SHARED && !hasWaitingRequests()) {
                    // We know there are no exclusive lock requests waiting; if
                    // there were any exclusive requesters then we would have
                    // at least one waiting request.
                    owners.add(ownerID);
                }
                else {
                    // Either the lock mode isn't shared, or there are pending
                    // exclusive requests.  Can't immediately acquire the lock.
                    LockRequest request = new LockRequest(ownerID, mode);
                    synchronized (request) {
                        waitingRequests.add(request);
                        // TODO request.wait();
                    }
                }
            }
            else {
                // Current mode is exclusive.  Whatever this request is,
                // it will have to wait.
                LockRequest request = new LockRequest(ownerID, mode);
                synchronized (request) {
                    waitingRequests.add(request);
                    // TODO request.wait();
                }
            }
        }


        public void release(int ownerID) {
            if (!owners.remove(ownerID)) {
                throw new IllegalStateException("Owner " + ownerID +
                    " didn't hold a lock on the entity!");
            }
            
            // If nobody is currently holding the lock, grant the lock to at
            // least one of the pending requests 
            if (owners.isEmpty() && hasWaitingRequests()) {
                LockRequest request = waitingRequests.getFirst();
                synchronized (request) {
                    owners.add(request.ownerID);
                    currentMode = request.mode;
                    request.notify();
                }

                if (currentMode == LockMode.SHARED) {
                    while (true) {
                        request = waitingRequests.peekFirst();
                        synchronized (request) {
                            if (request.mode == LockMode.SHARED) {
                                waitingRequests.removeFirst();
                                owners.add(request.ownerID);
                                request.notify();
                            }
                            else {
                                // This request is an exclusive lock.  Can't
                                // add any more owners to the lock.
                                break;
                            }
                        }
                    }
                }
            }
        }


        public boolean hasWaitingRequests() {
            return !waitingRequests.isEmpty();
        }
    }

    
    /**
     * This is the monitor used to synchronize and suspend threads when trying
     * to acquire locks.
     */
    private Object monitor = new Object();


    /**
     * This table records the currently locked entities, along with the
     * outstanding requests 
     */
    private final HashMap<Object, LockedEntity> lockTable;


    /** This collection records the list of locks that each owner holds. */
    private HashMap<Integer, ArrayList<LockedEntity>> ownerLocks;


    public LockManager() {
        lockTable = new HashMap<Object, LockedEntity>();
    }



    /**
     *
     * @param transactionID
     * @param entity
     * @param mode
     */
    public void acquireLock(int transactionID, Object entity, LockMode mode) {
        Semaphore sem;
        synchronized (lockTable) {
            // Get the entry for this lockable entity, creating one if necessary.
            LockedEntity entry = lockTable.get(entity);
            if (entry == null) {
                entry = new LockedEntity(entity, transactionID, mode);
                lockTable.put(entity, entry);
            }

            // TODO sem = entry.acquireLock();
        }

        // TODO sem.acquire();
    }
    
    
}

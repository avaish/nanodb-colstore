package edu.caltech.nanodb.transactions;


/**
 */
public class TransactionManager {

    /** This is the singleton instance of the transaction manager. */
    private static TransactionManager txnManager = null;



    /**
     * Returns the singleton instance of the transaction manager.
     *
     * @return the singleton instance of the transaction manager
     */
    public static TransactionManager getInstance() {
        if (txnManager == null) {
            throw new IllegalStateException(
                "TransactionManager has not been initialized");
        }

        return txnManager;
    }
}

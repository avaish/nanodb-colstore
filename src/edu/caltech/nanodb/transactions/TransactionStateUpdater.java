package edu.caltech.nanodb.transactions;


import edu.caltech.nanodb.commands.Command;
import edu.caltech.nanodb.server.CommandEventListener;
import edu.caltech.nanodb.server.EventDispatchException;


/**
 *
 */
public class TransactionStateUpdater implements CommandEventListener {

    @Override
    public void beforeCommandExecuted(Command cmd) throws EventDispatchException {
        // TODO:  Check if a new transaction needs to be started.
    }

    @Override
    public void afterCommandExecuted(Command cmd) throws EventDispatchException {
        // TODO:  Check if the transaction needs to be auto-committed.
    }
}

package edu.caltech.nanodb.commands;


/**
 * Abstract base-class for all commands that NanoDB supports.  Command classes
 * contain both the arguments and configuration details for the command being
 * executed, as well as the code for actually performing the command.  Databases
 * tend to have large <tt>switch</tt> statements controlling how various
 * commands are handled, and this really isn't a very pretty way to do things.
 * So, NanoDB uses a class-hierarchy for command representation and execution.
 * <p>
 * The command class is subclassed into various command categories that relate
 * to various operations in the database.  For example, the {@link QueryCommand}
 * class represents all <tt>SELECT</tt>, <tt>INSERT</tt>, <tt>UPDATE</tt>, and
 * <tt>DELETE</tt> operations.
 */
public abstract class Command {
    /**
     * Commands are either Data-Definition Language (DDL), Data-Manipulation
     * Language (DML), or utility commands.
     */
    public enum Type {
        /** A Data Definition Language (DDL) command. */
        DDL,

        /** A Data Manipulation Language (DML) command. */
        DML,

        /** A utility command. */
        UTILITY
    }


    /** The type of this command. */
    private Type cmdType;


    /**
     * Create a new command instance, of the specified command-type.  The
     * constructor is protected, but that is redundant with the fact that the
     * class is abstract anyways, so this class cannot be constructed directly.
     *
     * @param cmdType the general category of command
     */
    protected Command(Type cmdType) {
        this.cmdType = cmdType;
    }


    /**
     * Actually performs the command.
     *
     * @throws ExecutionException if an issue occurs during command execution
     */
    public abstract void execute() throws ExecutionException;
}

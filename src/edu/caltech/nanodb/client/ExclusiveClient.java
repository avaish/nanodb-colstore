package edu.caltech.nanodb.client;


import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import antlr.InputBuffer;
import antlr.LexerSharedInputState;
import antlr.RecognitionException;
import antlr.TokenStreamException;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.Command;
import edu.caltech.nanodb.commands.ExecutionException;
import edu.caltech.nanodb.commands.ExitCommand;

import edu.caltech.nanodb.sqlparse.NanoSqlLexer;
import edu.caltech.nanodb.sqlparse.NanoSqlParser;

import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class is used for starting the NanoDB database in exclusive mode, where
 * only a single client interacts directly with the database system.
 */
public class ExclusiveClient {

    public static final String LOGGING_CONF_FILE = "logging.conf";


    public static final String CMDPROMPT_FIRST = "CMD> ";


    public static final String CMDPROMPT_NEXT = "   > ";


    /**
     * Because this class is the entry-point of the database system, the logger
     * instance must be initialized <i>after</i> the logging system has been
     * initialized.  Thus we set this value to <code>null</code> initially, and
     * then initialize it later.
     */
    private static Logger logger = Logger.getLogger(ExclusiveClient.class);


    /**
     * This class provides a simple wrapper around the NanoSQL Lexer so that the
     * program can present a more user-friendly prompt for multi-line command
     * input.
     */
    private static class InteractiveLexer extends NanoSqlLexer {

        public InteractiveLexer(InputStream in) { super(in); }
        public InteractiveLexer(Reader in) { super(in); }
        public InteractiveLexer(InputBuffer ib) { super(ib); }
        public InteractiveLexer(LexerSharedInputState state) { super(state); }

        private boolean ignoreNewline = false;

        public void ignoreNextNewline() {
            ignoreNewline = true;
        }

        public void newline() {
            super.newline();

            if (ignoreNewline)
                ignoreNewline = false;
            else
                System.out.print(CMDPROMPT_NEXT);
        }
    }


    public static void main(String args[]) {
        // Start up the various database subsystems that require initialization.
        if (!startup()) {
            System.out.println("DATABASE STARTUP FAILED.");
            System.exit(1);
        }

        System.out.println("Welcome to NanoDB.  Exit with EXIT or QUIT command.\n");

        DataInputStream input = new DataInputStream(System.in);
        //NanoSqlLexer lexer = new NanoSqlLexer(input);
        InteractiveLexer lexer = new InteractiveLexer(input);
        NanoSqlParser parser = new NanoSqlParser(lexer);

        boolean firstCommand = true;
        while (true) {
            try {
                if (firstCommand)
                    firstCommand = false;
                else
                    lexer.ignoreNextNewline();

                System.out.print(CMDPROMPT_FIRST);
                Command cmd = parser.command();
                logger.debug("Parsed command:  " + cmd);

                if (cmd == null || cmd instanceof ExitCommand)
                    break;
                else
                    cmd.execute();
            }
            catch (RecognitionException e) {
                System.out.println("Parser error:  " + e.getMessage());
                logger.error("Parser error", e);
            }
            catch (TokenStreamException e) {
                System.out.println("Input stream error:  " + e.getMessage());
                logger.error("Input stream error", e);
            }
            catch(ExecutionException e) {
                System.out.println("Execution error:  " + e.getMessage());
                logger.error("Execution error", e);
            }
            catch (Throwable t) {
                System.out.println("Unexpected error:  " + t.getMessage());
                logger.error("Unexpected error", t);
            }

            // Persist all database changes.
            try {
                StorageManager.getInstance().closeAllOpenTables();
            }
            catch (IOException e) {
                System.out.println("IO error while closing open tables:  " +
                    e.getMessage());
                logger.error("IO error while closing open tables", e);
            }
        }

        // Shut down the various database subsystems that require cleanup.
        if (!shutdown()) {
            System.out.println("DATABASE SHUTDOWN FAILED.");
            System.exit(2);
        }
    }


    public static boolean startup() {
        // Initialize logging output first, since everything else uses logging!
        //PropertyConfigurator.configure(LOGGING_CONF_FILE);

        System.out.println("Initializing storage manager.");
        try {
            StorageManager.init();
        }
        catch (IOException ioe) {
            System.out.println("FAILED:");
            ioe.printStackTrace(System.out);
            return false;
        }

        // If we got here then everything initialized successfully.
        return true;
    }


    private static boolean shutdown() {
        System.out.println("Shutting down storage manager.");
        try {
            StorageManager.shutdown();
        }
        catch (IOException ioe) {
            System.out.println("FAILED:");
            ioe.printStackTrace(System.out);
            return false;
        }

        return true;
    }
}

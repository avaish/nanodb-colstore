package edu.caltech.nanodb.storage;


import edu.caltech.nanodb.client.SessionState;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The buffer manager reduces the number of disk IO operations by managing an
 * in-memory cache of data pages.
 *
 * @todo Add integrity checks, e.g. to make sure every cached page also appears
 *       in the collection of cached files.
 *
 * @todo Provide ways to close out files, i.e. flush them from the file-cache.
 */
public class BufferManager {

    /**
     * The system property that can be used to specify the size of the page
     * cache in the buffer manager.
     */
    public static final String PROP_PAGECACHE_SIZE = "nanodb.pagecache.size";

    /** The default page-cache size is defined to be 1MB. */
    public static final long DEFAULT_PAGECACHE_SIZE = 1024 * 1024;


    /**
     * The system property that can be used to specify the page replacement
     * policy in the buffer manager.
     */
    public static final String PROP_PAGECACHE_POLICY = "nanodb.pagecache.policy";


    /**
     * This helper class keeps track of a data page that is currently cached.
     */
    private static class CachedPageInfo {
        public DBFile dbFile;

        public int pageNo;

        public CachedPageInfo(DBFile dbFile, int pageNo) {
            if (dbFile == null)
                throw new IllegalArgumentException("dbFile cannot be null");

            this.dbFile = dbFile;
            this.pageNo = pageNo;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CachedPageInfo) {
                CachedPageInfo other = (CachedPageInfo) obj;
                return dbFile.equals(other.dbFile) && pageNo == other.pageNo;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + dbFile.hashCode();
            hash = 31 * hash + pageNo;
            return hash;
        }
    }


    /**
     * This helper class keeps track of a data page that is currently "pinned"
     * or in use by a client.  This prevents the page from being flushed out
     * of the cache while the client is using it.
     */
    private static class PinnedPageInfo {
        /** The session ID of the session that has this page pinned. */
        public int sessionID;

        /** The page that is pinned. */
        public DBPage dbPage;


        public PinnedPageInfo(int sessionID, DBPage dbPage) {
            this.sessionID = sessionID;
            this.dbPage = dbPage;
        }


        @Override
        public boolean equals(Object obj) {
            if (obj instanceof PinnedPageInfo) {
                PinnedPageInfo other = (PinnedPageInfo) obj;
                return sessionID == other.sessionID &&
                    dbPage.equals(other.dbPage);
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + sessionID;
            hash = 31 * hash + dbPage.hashCode();
            return hash;
        }
    }


    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BufferManager.class);


    private FileManager fileManager;


    /**
     * This collection holds the {@link DBFile} objects corresponding to various
     * opened files the database is currently using.
     */
    private LinkedHashMap<String, DBFile> cachedFiles;


    /**
     * This collection holds database pages that the database is currently
     * working with, so that they don't continually need to be reloaded.
     */
    private LinkedHashMap<CachedPageInfo, DBPage> cachedPages;


    /**
     * This collection holds all pages that are pinned by various sessions
     * that are currently accessing the database.
     */
    private HashSet<PinnedPageInfo> pinnedPages;
    

    /**
     * This collection maps session IDs to the pages that each session has
     * pinned.
     */
    private HashMap<Integer, HashSet<PinnedPageInfo>> pinnedPagesBySessionID;


    /**
     * This field records how many bytes are currently cached, in total.  Note
     * that this does not currently keep track of clean copies of dirty pages.
     */
    private long totalBytesCached;


    /**
     * This field records the maximum allowed cache size.
     */
    private long maxCacheSize;


    public BufferManager(FileManager fileManager) {
        this.fileManager = fileManager;

        configureMaxCacheSize();

        cachedFiles = new LinkedHashMap<String, DBFile>();

        String replacementPolicy = configureReplacementPolicy();
        cachedPages = new LinkedHashMap<CachedPageInfo, DBPage>(16, 0.75f,
            "lru".equals(replacementPolicy));

        totalBytesCached = 0;
        
        pinnedPages = new HashSet<PinnedPageInfo>();
        pinnedPagesBySessionID = new HashMap<Integer, HashSet<PinnedPageInfo>>();
    }


    private void configureMaxCacheSize() {
        // Set the default up-front; it's just easier that way.
        maxCacheSize = DEFAULT_PAGECACHE_SIZE;

        String str = System.getProperty(PROP_PAGECACHE_SIZE);
        if (str != null) {
            str = str.trim().toLowerCase();

            long scale = 1;
            if (str.length() > 1) {
                char modifierChar = str.charAt(str.length() - 1);
                boolean removeModifier = true;
                if (modifierChar == 'k')
                    scale = 1024;
                else if (modifierChar == 'm')
                    scale = 1024 * 1024;
                else if (modifierChar == 'g')
                    scale = 1024 * 1024 * 1024;
                else
                    removeModifier = false;

                if (removeModifier)
                    str = str.substring(0, str.length() - 1);
            }

            try {
                maxCacheSize = Long.parseLong(str);
                maxCacheSize *= scale;
            }
            catch (NumberFormatException e) {
                logger.error(String.format(
                    "Could not parse page-cache size value \"%s\"; " +
                    "using default value of %d bytes",
                    System.getProperty(PROP_PAGECACHE_SIZE),
                    DEFAULT_PAGECACHE_SIZE));

                maxCacheSize = DEFAULT_PAGECACHE_SIZE;
            }
        }
    }


    private String configureReplacementPolicy() {
        String str = System.getProperty(PROP_PAGECACHE_POLICY);
        if (str != null) {
            str = str.trim().toLowerCase();

            if (!("lru".equals(str) || "fifo".equals(str))) {
                logger.error(String.format(
                    "Unrecognized value \"%s\" for page-cache replacement " +
                    "policy; using default value of LRU.",
                    System.getProperty(PROP_PAGECACHE_POLICY)));
            }
        }

        return str;
    }


    /**
     * Retrieves the specified {@link DBFile} from the buffer manager, if it has
     * already been opened.
     *
     * @param filename The filename of the database file to retrieve.  This
     *        should be ONLY the database filename, no path.  The path is
     *        expected to be relative to the database's base directory.
     *
     * @return the {@link DBFile} corresponding to the filename, if it has
     *         already been opened, or <tt>null</tt> if the file isn't currently
     *         open.
     */
    public DBFile getFile(String filename) {
        DBFile dbFile = cachedFiles.get(filename);

        logger.debug(String.format(
            "Requested file %s is%s in file-cache.",
            filename, (dbFile != null ? "" : " NOT")));

        return dbFile;
    }
    
    
    public void addFile(DBFile dbFile) {
        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        String filename = dbFile.getDataFile().getName();
        if (cachedFiles.containsKey(filename)) {
            throw new IllegalStateException(
                "File cache already contains file " + filename);
        }
        
        // TODO:  If we want to keep a cap on how many files are opened, we
        //        would do that here.

        logger.debug(String.format( "Adding file %s to file-cache.", filename));
        
        cachedFiles.put(filename, dbFile);
    }
    
    
    private void pinPage(DBPage dbPage) {
        // Make sure this page is pinned by the session so that we don't
        // flush it until the session is done with it.
        
        int sessionID = SessionState.get().getSessionID();
        PinnedPageInfo pp = new PinnedPageInfo(sessionID, dbPage);
        
        // First, add it to the overall set of pinned pages.
        
        if (pinnedPages.add(pp)) {
            dbPage.incPinCount();
            logger.debug(String.format("Session %d is pinning page [%s,%d].  " +
                "New pin-count is %d.", sessionID, dbPage.getDBFile(),
                dbPage.getPageNo(), dbPage.getPinCount()));
        }
        
        // Next, add it to the set of pages pinned by this particular session.
        // (This makes it easier to unpin all pages used by this session.)
        
        HashSet<PinnedPageInfo> pinnedBySession =
            pinnedPagesBySessionID.get(sessionID);
        
        if (pinnedBySession == null) {
            pinnedBySession = new HashSet<PinnedPageInfo>();
            pinnedPagesBySessionID.put(sessionID, pinnedBySession);
        }

        pinnedBySession.add(pp);
    }


    private void unpinPage(DBPage dbPage) {
        // If the page is pinned by the session then unpin it.
        int sessionID = SessionState.get().getSessionID();
        PinnedPageInfo pp = new PinnedPageInfo(sessionID, dbPage);

        // First, remove it from the overall set of pinned pages.
        
        if (pinnedPages.remove(pp)) {
            dbPage.decPinCount();
            logger.debug(String.format("Session %d is unpinning page " +
                "[%s,%d].  New pin-count is %d.", sessionID, dbPage.getDBFile(),
                dbPage.getPageNo(), dbPage.getPinCount()));
        }

        // Next, remove it from the set of pages pinned by this particular
        // session.

        HashSet<PinnedPageInfo> pinnedBySession =
            pinnedPagesBySessionID.get(sessionID);
        
        if (pinnedBySession != null) {
            pinnedBySession.remove(pp);

            // If the set becomes empty, remove the hash-set for the session.
            if (pinnedBySession.isEmpty())
                pinnedPagesBySessionID.remove(sessionID);
        }
    }


    public DBPage getPage(DBFile dbFile, int pageNo) {
        DBPage dbPage = cachedPages.get(new CachedPageInfo(dbFile, pageNo));

        logger.debug(String.format(
            "Requested page [%s,%d] is%s in page-cache.",
            dbFile, pageNo, (dbPage != null ? "" : " NOT")));

        if (dbPage != null) {
            // Make sure this page is pinned by the session so that we don't
            // flush it until the session is done with it.
            pinPage(dbPage);
        }

        return dbPage;
    }


    public void addPage(DBPage dbPage) throws IOException {
        if (dbPage == null)
            throw new IllegalArgumentException("dbPage cannot be null");

        DBFile dbFile = dbPage.getDBFile();
        int pageNo = dbPage.getPageNo();

        CachedPageInfo cpi = new CachedPageInfo(dbFile, pageNo);
        if (cachedPages.containsKey(cpi)) {
            throw new IllegalStateException(String.format(
                "Page cache already contains page [%s,%d]", dbFile, pageNo));
        }
        
        logger.debug(String.format( "Adding page [%s,%d] to page-cache.",
            dbFile, pageNo));

        int pageSize = dbPage.getPageSize();
        if (pageSize + totalBytesCached > maxCacheSize && !cachedPages.isEmpty()) {
            // The cache will be too large after adding this page.  Try to solve
            // this problem by evicting pages.

            Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
                cachedPages.entrySet().iterator();

            while (entries.hasNext() &&
                   pageSize + totalBytesCached > maxCacheSize) {
                Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

                DBPage oldPage = entry.getValue();

                if (oldPage.isPinned())  // Can't flush pages that are in use.
                    continue;

                logger.debug(String.format(
                    "    Evicting page [%s,%d] from page-cache to make room.",
                    oldPage.getDBFile(), oldPage.getPageNo()));

                if (oldPage.isDirty()) {
                    logger.debug("    Evicted page is dirty; saving to disk.");
                    fileManager.saveDBPage(oldPage);
                }

                entries.remove();
                totalBytesCached -= oldPage.getPageSize();
                oldPage.invalidate();
            }
        }

        cachedPages.put(cpi, dbPage);

        // Make sure this page is pinned by the session so that we don't flush
        // it until the session is done with it.
        pinPage(dbPage);
    }


    public void flushDBFile(DBFile dbFile) throws IOException {
        logger.info("Flushing all pages for file " + dbFile +
            " from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            CachedPageInfo info = entry.getKey();
            if (dbFile.equals(info.dbFile)) {
                DBPage oldPage = entry.getValue();

                logger.debug(String.format(
                    "    Evicting page [%s,%d] from page-cache.",
                    oldPage.getDBFile(), oldPage.getPageNo()));

                if (oldPage.isDirty()) {
                    logger.debug("    Evicted page is dirty; saving to disk.");
                    fileManager.saveDBPage(oldPage);
                }

                // Remove the page from the cache.
                entries.remove();
                totalBytesCached -= oldPage.getPageSize();
                oldPage.invalidate();
            }
        }
    }


    public void flushAll() throws IOException {
        logger.info("Flushing ALL database pages from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            DBPage oldPage = entry.getValue();

            logger.debug(String.format(
                "    Evicting page [%s,%d] from page-cache.",
                oldPage.getDBFile(), oldPage.getPageNo()));

            if (oldPage.isDirty()) {
                logger.debug("    Evicted page is dirty; saving to disk.");
                fileManager.saveDBPage(oldPage);
            }

            // Remove the page from the cache.
            entries.remove();
            totalBytesCached -= oldPage.getPageSize();
            oldPage.invalidate();
        }
    }
    
    
    public void removeDBFile(DBFile dbFile) throws IOException {
        logger.info("Removing DBFile " + dbFile + " from buffer manager");
        flushDBFile(dbFile);
        cachedFiles.remove(dbFile.getDataFile().getName());
    }
}

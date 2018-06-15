package raft;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public class Log {

    public static RaftLog newLog(Storage storage, Logger logger) {
        if (storage == null) logger.panic("storage must not be null");
        RaftLog log = new RaftLog(storage, logger);

        long firstIndex = storage.FirstIndex();
        long lastIndex  = storage.LastIndex();

        log.unstable.offset = lastIndex + 1;
        log.unstable.logger = logger;

        // Initialize our committed and applied pointers to the time of the last compaction.
        log.committed = firstIndex - 1;
        log.applied = firstIndex - 1;

        return log;
    }

}

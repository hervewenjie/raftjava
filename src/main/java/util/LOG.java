package util;

import raft.Logger;
import raft.LoggerImpl;

/**
 * Created by chengwenjie on 2018/6/15.
 */
public class LOG {
    static Logger log = new LoggerImpl();

    public static void info(String s) {log.info(s);}
    public static void warn(String s) {log.warning(s);}
    public static void error(String s) {log.error(s);}
    public static void debug(String s) {log.debug(s);}
    public static void panic(String s) {log.panic(s);}
}

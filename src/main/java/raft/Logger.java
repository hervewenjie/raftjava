package raft;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public interface Logger {
    void debug(String s);
    void error(String s);
    void info(String s);
    void warning(String s);
    void panic(String s);
}

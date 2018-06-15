package raft;

/**
 * Created by chengwenjie on 2018/6/1.
 */
public class LoggerImpl implements Logger {
    @Override
    public void debug(String s) {
        System.out.println("[DEBUG] " + s);
    }

    @Override
    public void info(String s) {
        System.out.println("[INFO] " + s);
    }

    @Override
    public void warning(String s) {
        System.out.println("[WARN] " + s);
    }

    @Override
    public void error(String s) {
        System.err.println("[ERRO] " + s);
    }

    @Override
    public void panic(String s) {
        System.err.println("[PANIC] " + s);
        System.exit(1);
    }
}

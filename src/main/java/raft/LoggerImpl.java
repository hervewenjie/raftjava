package raft;

/**
 * Created by chengwenjie on 2018/6/1.
 */
public class LoggerImpl implements Logger {
    @Override
    public void debug(String s) {
        System.out.println("[DEBUG] " + Thread.currentThread().getName() + " " + s);
    }

    @Override
    public void info(String s) {
        System.out.println("[INFO ] " + Thread.currentThread().getName() + " " + s);
    }

    @Override
    public void warning(String s) {
        System.out.println("[WARN ] " + Thread.currentThread().getName() + " " + s);
    }

    @Override
    public void error(String s) {
        System.err.println("[ERROR] " + Thread.currentThread().getName() + " " + s);
    }

    @Override
    public void panic(String s) {
        System.err.println("[PANIC] " + Thread.currentThread().getName() + " " + s);
        System.exit(1);
    }
}

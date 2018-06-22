package util;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public class Panic {
    public static void panic(String s) {
        System.err.println(Thread.currentThread().getName() + " " + s);
        System.exit(1);
    }
}

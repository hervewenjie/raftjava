package util;

/**
 * Created by chengwenjie on 2018/5/30.
 */
public class Panic {
    public static void panic(String s) {
        System.err.println("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)");
        System.exit(1);
    }
}

package example;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by chengwenjie on 2018/6/12.
 */
public class Test {

    static Queue q = new ArrayBlockingQueue(128);

    public static void main(String[] args) {

        new Thread(() -> {
            while (true) {
                if (q.peek() == null) {
                    System.out.println("consumer peek empty");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {}
                } else {
                    System.out.println("consumer !!! " + q.poll());
                    break;
                }
            }
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {}
            q.add(new Object());
        }).start();
    }
}

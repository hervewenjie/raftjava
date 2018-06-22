package time;

/**
 * Created by chengwenjie on 2018/6/13.
 */
public class Ticker {

    static long TIMEOUT = 3000;

    private long start;

    public Ticker() {
        start = System.currentTimeMillis();
    }

    public boolean isTimeout() {
        long now = System.currentTimeMillis();
        if (now - start > TIMEOUT) {
            start = now;
            return true;
        }
        return false;
    }

    public static void main(String[] args) {
        Ticker ticker = new Ticker();
        while (true) {
            if (ticker.isTimeout()) {
                System.out.println("timeout...");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
        }
    }
}

package pb;

/**
 * Created by chengwenjie on 2018/6/14.
 */
public class PbUtil {

    public static byte[] MustMarshal(Marshaler m) {
        byte[] d = m.Marshal();
        return d;
    }
}

package util;

import pb.Entry;
import pb.Message;

import java.util.List;

/**
 * Created by chengwenjie on 2018/6/14.
 */
public class ArrayUtil {

    public static Entry[] merge(Entry[] a, Entry[] b) {
        if (a == null || a.length == 0) return b;
        if (b == null || b.length == 0) return a;
        Entry[] c = new Entry[a.length + b.length];
        int i;
        for (i = 0; i < a.length; i++) {c[i] = a[i];}
        for (int j = 0; j < b.length; j++) {
            c[i++] = b[j];
        }
        return c;
    }

    public static Message[] toArray(List<Message> l) {
        if (l == null || l.size() == 0) return new Message[0];
        Message[] m = new Message[l.size()];
        for (int i = 0; i < m.length; i++) m[i] = l.get(i);
        return m;
    }


}

package util;

import example.KV;

import java.io.*;

/**
 * Created by chengwenjie on 2018/6/12.
 */
public class Encoder {

    public static byte[] encode(KV kv) {
        return (kv.key + "," + kv.val).getBytes();
    }

    public static KV decode(byte[] bytes) {
        String s = new String(bytes);
        String[] l = s.split(",");
        KV kv = new KV(l[0], l[1]);
        return kv;
    }

    public static void main(String[] args) {
        KV kv = new KV("hello", "world");
        byte[] bytes = encode(kv);
        KV kv1 = decode(bytes);
        System.out.println("after decode key " + kv1.key);
        System.out.println("after decode value " + kv1.val);
    }

    public static byte[] toByteArray(Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    public static Object toObject(byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return obj;
    }
}

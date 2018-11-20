package misc;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Hasher implements Serializable {

    private MessageDigest md;

    public byte[] hash(String s) {
        if (md == null) {
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        } else {
            md.reset();
        }

        md.update(s.getBytes());
        return md.digest();
    }

}

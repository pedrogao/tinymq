package github.io.pedrogao.diskqueue.util;

public class MathUtil {

    /**
     * mod by shift
     *
     * @param val  input value
     * @param bits number of bit
     * @return divider
     */
    public static long mod(long val, int bits) {
        return val - ((val >> bits) << bits);
    }

    /**
     * multiply by shift
     *
     * @param val  input value
     * @param bits number of bit
     * @return result
     */
    public static long mul(long val, int bits) {
        return val << bits;
    }

    /**
     * divide by shift
     *
     * @param val  input value
     * @param bits number of bit
     * @return result
     */
    public static long div(long val, int bits) {
        return val >> bits;
    }
}

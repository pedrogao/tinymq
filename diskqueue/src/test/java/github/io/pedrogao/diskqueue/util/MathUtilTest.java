package github.io.pedrogao.diskqueue.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MathUtilTest {

    @Test
    void mod() {
        assertEquals(MathUtil.mod(10, 3), 2);
    }

    @Test
    void mul() {
        assertEquals(MathUtil.mul(10, 3), 80);
    }

    @Test
    void div() {
        assertEquals(MathUtil.div(10, 2), 2);
    }
}
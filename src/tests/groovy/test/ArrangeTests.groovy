package test

import tidyflow.exception.KeySetMismatchException

import static groovy.test.GroovyAssert.shouldFail
import static tidyflow.Methods.*

class ArrangeTests {

    private static df = data_frame(
        x: [1,1,2,2,3,4,5],
        y: [5,5,4,4,3,2,1],
        z: [true, false, false, true, false, false, false])

    private static String name = 'arrange'

    static void arrangeTests() {
        test_01()
        test_02()
    }

    static void test_01() {
        final String num = '01'

        assert df.arrange('x').pull('x') == [1,1,2,2,3,4,5]
        assert df.arrange('y').pull('y') == [1,2,3,4,4,5,5]
        assert df.arrange('y', desc:true).pull('y') == [5,5,4,4,3,2,1]
        assert df.arrange('x', 'z').pull('z') == [false, true, false, true, false, false, false]

        println "$name test $num - pass"
    }

    static void test_02() {
        final String num = '02'

        shouldFail (KeySetMismatchException) {
            df.arrange('a', 'b', 'c')
        }

        println "$name test $num - pass"
    }


}

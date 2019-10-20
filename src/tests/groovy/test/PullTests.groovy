package test


import tidyflow.exception.KeySetMismatchException

import static groovy.test.GroovyAssert.shouldFail
import static tidyflow.Methods.*

class PullTests {

    private static df = data_frame(
        x: [1,2,3,4,5],
        y: [5,4,3,2,1],
        z: ['a','b','c','d','e'])

    private static String name = 'pull'

    static void pullTests() {
        test_01()
        test_02()
    }

    static void test_01() {
        final String num = '01'

        assert df.pull('x') == [1,2,3,4,5]

        println "$name test $num - pass"
    }

    static void test_02() {
        final String num = '02'

        shouldFail (KeySetMismatchException) {
            df.pull('a')
        }

        println "$name test $num - pass"
    }


}

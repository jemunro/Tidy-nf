package test

import tidyflow.exception.IllegalTypeException

import static groovy.test.GroovyAssert.shouldFail
import static tidyflow.Methods.*

class SliceTests {

    private static df = as_df(
        x: [1,2,3,4,5],
        y: [5,4,3,2,1],
        z: ['a','b','c','d','e'])

    private static String name = 'slice'

    static void sliceTests() {
        test_01()
        test_02()
    }

    static void test_01() {
        final String num = '01'

        assert df.slice(0)['x'] == [1]
        assert df.slice(2,4)['y'] == [3,1]
        assert df.slice(0..<df.nrow())['x'] == [1,2,3,4,5]

        println "$name test $num - pass"
    }

    static void test_02() {
        final String num = '02'

        shouldFail (IndexOutOfBoundsException) {
            df.slice(1,5)
        }

        shouldFail (IndexOutOfBoundsException) {
            df.slice(-1)
        }

        shouldFail (IllegalTypeException) {
            df.slice(['a'])
        }

        println "$name test $num - pass"
    }


}

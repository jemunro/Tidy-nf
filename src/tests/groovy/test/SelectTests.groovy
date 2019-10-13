package test

import tidyflow.exception.KeySetMismatchException

import static groovy.test.GroovyAssert.shouldFail
import static tidyflow.TidyMethods.*

class SelectTests {

    private static df = as_df(
        x: [1,2,3,4,5],
        y: [5,4,3,2,1],
        z: ['a','b','c','d','e'])

    private static String name = 'select'

    static void selectTests() {
        test_01()
        test_02()
    }

    static void test_01() {
        final String num = '01'

        assert df.select('z', 'y', 'z').names() == ['z', 'y', 'z'] as LinkedHashSet
        assert df.select('x', 'z').names() == ['x', 'z'] as LinkedHashSet
        assert df.select('y').names() == ['y'] as LinkedHashSet

        println "$name test $num - pass"
    }

    static void test_02() {
        final String num = '02'

        shouldFail (KeySetMismatchException) {
            df.select('a', 'b', 'c')
        }

        shouldFail (KeySetMismatchException) {
            df.transpose().select('a', 'b', 'c')
        }

        println "$name test $num - pass"
    }


}

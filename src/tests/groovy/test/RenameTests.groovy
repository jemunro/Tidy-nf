package test

import tidyflow.exception.KeySetMismatchException

import static groovy.test.GroovyAssert.shouldFail
import static tidyflow.TidyMethods.*

class RenameTests {

    private static df = as_df(
        x: [1,2,3,4,5],
        y: [5,4,3,2,1],
        z: ['a','b','c','d','e'])

    private static String name = 'rename'

    static void renameTests() {
        test_01()
        test_02()
    }

    static void test_01() {
        final String num = '01'

        assert df.rename(a: 'x').names() == ['a', 'y', 'z'] as LinkedHashSet
        assert df.rename(b:'y', c:'z').names() == ['x', 'b', 'c'] as LinkedHashSet

        println "$name test $num - pass"
    }

    static void test_02() {
        final String num = '02'

        shouldFail (KeySetMismatchException) {
            df.rename(z: 'a')
        }

        shouldFail (KeySetMismatchException) {
            df.rename(a: 'q')
        }

        println "$name test $num - pass"
    }


}

package test

import tidyflow.dataframe.DataFrame
import tidyflow.exception.KeySetMismatchException

import static groovy.test.GroovyAssert.shouldFail
import static tidyflow.Methods.*

class NestTests {

    private static df = data_frame(
        x: [1,2,3,4,5,6],
        y: ['a','b','c','a','b','a'],
        z: [true, false, true, false, true, false])

    private static String name = 'nest'

    static void nestTests() {
        test_01()
        test_02()
    }

    static void test_01() {
        final String num = '01'

        assert (df.nest_by('y').pull('x') as Set) == ([[1,4,6], [2,5], [3]] as Set)
        assert (df.nest_by('y', 'z').pull('x') as Set) == ([[1], [2], [3], [4, 6], [5]] as Set)

        println "$name test $num - pass"
    }

    static void test_02() {
        final String num = '02'

        shouldFail (KeySetMismatchException) {
            df.nest_by('foo')
        }

        shouldFail (KeySetMismatchException) {
            df.nest_by('y', 'foo')
        }

        println "$name test $num - pass"
    }


}

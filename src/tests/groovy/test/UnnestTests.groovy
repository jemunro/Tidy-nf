package test

import tidyflow.dataframe.DataFrame
import tidyflow.exception.KeySetMismatchException

import static groovy.test.GroovyAssert.shouldFail
import static tidyflow.Methods.*

class UnnestTests {

    private static df = data_frame(
        x: [1,2,3,4,5,6],
        y: ['a','b','c','a','b','a'],
        z: [true, false, true, false, true, false])

    private static String name = 'unnest'

    static void unnestTests() {
        test_01()
        test_02()
    }

    static void test_01() {
        final String num = '01'

        assert  df.nest_by('y').unnest('x', 'z').select('x', 'y', 'z').arrange('x').as_list() ==
                df.as_list()
        assert  df.nest_by('y', 'z').unnest('x').select('x', 'y', 'z').arrange('x').as_list() ==
                df.as_list()

        println "$name test $num - pass"
    }

    static void test_02() {
        final String num = '02'

        shouldFail (KeySetMismatchException) {
            df.unnest('foo')
        }

        println "$name test $num - pass"
    }


}

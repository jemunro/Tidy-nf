package test


import static groovy.test.GroovyAssert.shouldFail
import static tidyflow.Methods.*

class MutateTests {

    private static df = as_df(
        x: [1,2,3,4,5],
        y: [5,4,3,2,1],
        z: ['a','b','c','d','e'])

    private static String name = 'mutate'

    static void mutateTests() {
        test_01()
        test_02()
    }

    static void test_01() {
        final String num = '01'

        assert df.mutate { x = x + 1 }['x'] == [2,3,4,5,6]
        assert df.mutate { w = y - 1 }['w'] == [4,3,2,1,0]
        assert df.mutate_with(a:1) { w = y - a }['w'] == [4,3,2,1,0]

        println "$name test $num - pass"
    }

    static void test_02() {
        final String num = '02'

        shouldFail (NullPointerException) {
            df.mutate { x = null + x }
        }

        shouldFail (MissingPropertyException) {
            df.mutate { a = b }
        }

        shouldFail (MissingPropertyException) {
            df.mutate_with(a:1) { a = b }
        }

        println "$name test $num - pass"
    }


}

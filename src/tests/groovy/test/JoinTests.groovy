package test

import tidyflow.exception.KeySetMismatchException

import static groovy.test.GroovyAssert.shouldFail
import static tidyflow.Methods.*

class JoinTests {

    private static left = as_df(
        x: [1,2,3],
        y: ['a','b','c'])

    private static right = as_df(
        x: [1,3,5],
        z: ['one','three','five'])

    private static String name = 'join'

    static void joinTests() {
        test_01()
        test_02()
    }

    static void test_01() {
        final String num = '01'

        assert (left.inner_join(right, 'x')['x'] as Set) == ([1,3] as Set)
        assert (left.left_join(right, 'x')['x'] as Set) == ([1,2,3] as Set)
        assert (left.right_join(right, 'x')['x'] as Set) == ([1,3,5] as Set)
        assert (left.full_join(right, 'x')['x'] as Set) == ([1,2,3,5] as Set)

        println "$name test $num - pass"
    }

    static void test_02() {
        final String num = '02'

        shouldFail (KeySetMismatchException) {
            left.inner_join(right, 'x', 'q')
        }

        shouldFail (KeySetMismatchException) {
            left.inner_join(right, 'x', 'y')
        }

        println "$name test $num - pass"
    }


}

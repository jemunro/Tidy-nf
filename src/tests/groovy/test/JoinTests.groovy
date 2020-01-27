package test

import tidyflow.exception.KeySetMismatchException

import static groovy.test.GroovyAssert.shouldFail
import static tidyflow.Methods.*

class JoinTests {

    private static left = data_frame(
        x: [1,2,3],
        y: ['a','b','c'])

    private static right = data_frame(
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

        assert (left.semi_join(right, 'x')['x'] as Set) == ([1,3] as Set)
        assert (left.semi_join(right, 'x').colnames()) == (['x', 'y'] as Set)

        assert (left.anti_join(right, 'x')['x'] as Set) == ([2] as Set)
        assert (left.anti_join(right, 'x').colnames()) == (['x', 'y'] as Set)

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

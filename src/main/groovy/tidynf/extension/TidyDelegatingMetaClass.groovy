package tidynf.extension

import groovy.runtime.metaclass.NextflowDelegatingMetaClass

class TidyDelegatingMetaClass extends NextflowDelegatingMetaClass {
    Class operators

    TidyDelegatingMetaClass(MetaClass delegate, Class operators) {
        super(delegate)
        this.operators = operators
    }

    @Override
    Object invokeMethod(Object object, String method, Object[] arguments) {
        def args = arguments as ArrayList

        if (arguments.size() > 0 && arguments[0] instanceof LinkedHashMap) {
            args.add(1, object)
        } else {
            args.add(0, object)
        }

        if (operators.metaClass.respondsTo(operators, method, *args)) {
            operators."$method"(*args)
        } else {
            super.invokeMethod(object, method, arguments)
        }
    }
}
package channelextra
import groovy.runtime.metaclass.NextflowDelegatingMetaClass

class ChannelExtraDelegatingMetaClass extends NextflowDelegatingMetaClass {
    Class[] operators

    ChannelExtraDelegatingMetaClass(MetaClass delegate) {
        super(delegate)
        this.operators = new Class[1]
        this.operators[0] = ChannelExtraOperators
    }

    ChannelExtraDelegatingMetaClass(MetaClass delegate, Class... operators) {
        super(delegate)
        this.operators = operators
    }

    @Override
    Object invokeMethod(Object object, String method, Object[] arguments) {
        def args = arguments as List
        if (arguments.size() > 0 && arguments[0] instanceof LinkedHashMap) {
            args.add(1, object)
        } else {
            args.add(0, object)
        }
        def match = operators.find { it.metaClass.respondsTo(it, method, *args) }
        if (match) {
            match."$method"(*args)
        } else {
            super.invokeMethod(object, method, arguments)
        }
    }
}
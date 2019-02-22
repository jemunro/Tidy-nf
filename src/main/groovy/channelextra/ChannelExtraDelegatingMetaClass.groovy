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
        def match = operators.find { it.metaClass.respondsTo(it, method, object, *arguments) }
        if (match) {
            match."$method"(object, *arguments)
        } else {
            super.invokeMethod(object, method, arguments)
        }
    }
}
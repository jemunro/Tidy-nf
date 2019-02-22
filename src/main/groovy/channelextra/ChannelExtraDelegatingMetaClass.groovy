package channelextra
import groovy.runtime.metaclass.NextflowDelegatingMetaClass

class ChannelExtraDelegatingMetaClass extends NextflowDelegatingMetaClass {
    Class operators

    ChannelExtraDelegatingMetaClass(MetaClass delegate) {
        super(delegate)
        this.operators = ChannelExtraOperators
    }

    ChannelExtraDelegatingMetaClass(MetaClass delegate, Class operatorClass) {
        super(delegate)
        if (operatorClass){
            this.operators = operatorClass
        } else {
            this.operators = ChannelExtraOperators
        }
    }

    @Override
    Object invokeMethod(Object object, String method, Object[] arguments) {
        if (operators.metaClass.respondsTo(operators, method, object, *arguments)) {
            operators."$method"(object, *arguments)
        } else {
            super.invokeMethod(object, method, arguments)
        }
    }
}
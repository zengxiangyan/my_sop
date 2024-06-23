from gremlin_python import statics
from gremlin_python.process.traversal import P
from graph import GDAO

class FullTextP(P):
    """Extend P with JanusGraph Full-Text search predicates"""
    def __init__(self, operator, value, other=None):
        P.__init__(self, operator, value, other)

    @staticmethod
    def textContains(*args):
        if GDAO.is_hugegraph():
            out = 'P.textcontains("{}")'.format(str(*args))
            print('out:', out)
            return out
            return "[ConditionP, textContains, {arg}]".format(arg=str(*args))
            # return "[biPredicate:{},value:{},originalValue:{}]".format('TEXT_CONTAINS', str(*args), str(*args))
        return FullTextP("textContains", *args)

    @staticmethod
    def textContainsFuzzy(*args):
        return FullTextP("textContainsFuzzy", *args)

    @staticmethod
    def textContainsPrefix(*args):
        return FullTextP("textContainsPrefix", *args)

    @staticmethod
    def textContainsRegex(*args):
        return FullTextP("textContainsRegex", *args)

    @staticmethod
    def textFuzzy(*args):
        return FullTextP("textFuzzy", *args)

    @staticmethod
    def textPrefix(*args):
        return FullTextP("textPrefix", *args)

    @staticmethod
    def textRegex(*args):
        return FullTextP("textRegex", *args)

def textContains(*args):
    return FullTextP.textContains(*args)
statics.add_static('textContains', textContains)

def textContainsFuzzy(*args):
    return FullTextP.textContainsFuzzy(*args)
statics.add_static('textContainsFuzzy', textContainsFuzzy)

def textContainsPrefix(*args):
    return FullTextP.textContainsPrefix(*args)
statics.add_static('textContainsPrefix', textContainsPrefix)

def textContainsRegex(*args):
    return FullTextP.textContainsRegex(*args)
statics.add_static('textContainsRegex', textContainsRegex)

def textFuzzy(*args):
    return FullTextP.textFuzzy(*args)
statics.add_static('textFuzzy', textFuzzy)

def textPrefix(*args):
    return FullTextP.textPrefix(*args)
statics.add_static('textPrefix', textPrefix)

def textRegex(*args):
    return FullTextP.textRegex(*args)
statics.add_static('textRegex', textRegex)

#from __future__ import print_function
import sys
import types


_IO_module = __import__('io' if sys.version_info >= (3,) else 'cStringIO')
_main_module = sys.modules['__main__']
_builtins = (_main_module.__builtins__).__dict__
_print = (_builtins)['print']


def _type_as_str(x) :
    cls = getattr(x,'__class__',)
    if cls is None:
        tp = type(x)
    else:
        tp = cls
    return tp.__name__


print_ID_always =   True

_global_print_map = ( ('',''), )


class BadTypeForCallback(RuntimeError): pass

#
# seen : prevent endless recursion
# indent : (start , increment) of indentation
# stream : output object ( STDOUT by default)
# types_callback:  a map from stringized classname to generator of multiline description


def myInspect( pyobj , seen = None, indent = (0,4), stream = sys.stdout, types_callback = None):

    if types_callback is None: types_callback = _global_print_map
    if seen is None: seen = dict()
    this_id = id(pyobj)
    this_seen = (seen.get(this_id) is not None)
    seen[this_id] = pyobj
    new_indent = list(indent)
    new_indent[0] += new_indent[1]
    ind_fn = lambda itup, spc: itup[0]*' ' + (spc+' ')[0]
    IndentedPrint=( lambda toPrint, spc='':
                    _print(ind_fn(indent,spc)+str(toPrint), file = stream) )

    if this_seen or print_ID_always:
        IndentedPrint('{} @ {}'.format(type(pyobj),"0x%x"%id(pyobj)),'&')

    num = 0
    if not this_seen:
        num += 1
    else:
        IndentedPrint(str(num),"~")
        return

    D_types_callback = dict( _global_print_map )

    if isinstance(types_callback, (dict, tuple)):
        D_types_callback.update(types_callback)
    else:
        raise BadTypeForCallback(
                  'myInspect() needs types_callback to be <tuple> or <dict>')

    myinspect = ( lambda obj : 
        myInspect( obj, seen, new_indent, stream, types_callback = D_types_callback)
    )

    if isinstance(pyobj, dict):

        # ___ HANDLE DICTIONARY ___

        IndentedPrint('DICT({})'.format(len(pyobj)),'*');

        for x,y in pyobj.items():
            IndentedPrint(repr(x) + " => ", ">")
            myinspect(y)

    elif isinstance( pyobj, (set,list,tuple)):

        # ___ HANDLE LIST, SET, TUPLE ___

        IndentedPrint(pyobj.__class__.__name__.upper()+"({})".format(len(pyobj)),'*')

        for x in pyobj:
            IndentedPrint(str(num),"~");num+=1
            myinspect(x)

    else: 

        # ___ HANDLE OBJECT INSTANCE ___

        Tclass = getattr(pyobj,'__class__',None) 

        if Tclass is None:
            IndentedPrint('{!s} - unsupported_type'.format(pyobj),'*')
        else:
            type_name = _type_as_str(pyobj)
            IndentedPrint('OBJECT of class "{}": '.format(type_name),'*')
            cbk = ( D_types_callback.get((pyobj).__class__,'') or \
                    D_types_callback.get(type_name,'')   )

            if cbk and callable(cbk): 
                for line in cbk(pyobj):
                    IndentedPrint(line,'|')
            else:
                _vars = []
                try:
                  pyobj.__dict__
                  _vars = [ n for n in dir(pyobj) if not n.startswith('__') ]
                except: _vars  = None
                if _vars is not None:
                    for x in _vars:
                        attr_x = getattr(pyobj,x,None)
                        IndentedPrint(x + " -> " ,'>') ; myinspect(attr_x)
                else:
                    IndentedPrint(repr(pyobj),'=')

    if indent[0]==0: _print('---',file=stream)


if __name__ == '__main__':

    class X(object): 
        def __init__(self,i) : self.i = i
    class Y: 
        def __init__(self,j,k=1) : self.j = j; self.k = k * 2
    class Z: 
        def __init__(self,j=0,k=1) : self.j = j; self.k = k 

    xx = X( Y( { 'a': [1, 0j, {3,4}] } ) ) 
    yy = Z()

    myInspect( xx)

    tc= ( #class name/type, and a callable / generator of lines of instance descriptive text 
          ['Y', lambda obj: ("Y with j={}".format(obj.j), "   and k={}".format(obj.k)) ], 
          [ Z , lambda obj: ("Z with j={}".format(obj.j),) ],
        )

    myInspect( (xx, yy), types_callback = tc)

    out_buf = _IO_module.StringIO()

    myInspect( Y( X( { 'a': [1, 0j, {3,4}] } ) ) , stream = out_buf )
    
    for x in _IO_module.StringIO( out_buf.getvalue() ):
        _print (">>>> " + x, end="")

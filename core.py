import datetime
import json
from genquery import AS_LIST, AS_DICT, row_iterator
import warnings
from textwrap import dedent

from bytes_unicode_mapper import to_bytes, to_unicode, map_recursively

from checkperms import ( user_id_for_name, 
                         check_perms_on_data_object,
                         check_perms_on_collection   )


def logger (callback,strm='serverLog'):
    return  lambda s: callback.writeLine( strm, s )

def delayed_container_launch(rule_args, callback, rei):

    # - perhaps set metadata on container config to indicate which input coll. to process

    (container_config, src_resc, dest_resc) = rule_args

    p = logger(callback,'stdout')
    input_dir =  ""

    remote_host = ""

    for x in row_iterator ('RESC_LOC',"RESC_NAME = '{}'".format(dest_resc),AS_LIST,callback):
        remote_host = x[0]

    p('inputdir={!r}'.format(input_dir))
    
    output_dir = ""

    x = dedent("""\
               remote ("{remote_host}","") {
                   handle_docker_call( "{container_config}",
               }
               """)
    callback.writeLine("stdout",x)
    callback.delayExec("<PLUSET>10s</PLUSET>", x ,"")

def create_collection (args,callback,rei):
    objpath=args[0]
    rv = callback.msiCollCreate (objpath, "0", 0)
    
def set_acl_inherit (args,callback,rei):
    objpath = args[0]
    user = args[1]
    rv = callback.msiSetACL ("recursive", "admin:inherit", user, objpath)

def set_acl (args,callback,rei):
    pr = make_logger (callback)
    objpath = args[0]
    user = args[1]
    rv = callback.msiSetACL ("default", "admin:own", user, objpath)

def pep_api_data_obj_repl_post(rule_args,callback,rei ):
  dataobjinp = rule_args[2]
  cI = dataobjinp.condInput; condInp = { str(cI.key[i]):str(cI.value[i]) for i in range(cI.len) }
  dest_resc = str( condInp['destRescName'] )
  obj_path = str( dataobjinp.objPath )

# -- DEBUG VERSION
#
#from myinspect import myInspect
#
#def pep_api_data_obj_repl_post(a,c,r ):
#    import cStringIO
#    out=cStringIO.StringIO() 
#    myInspect ( a, stream=out ,types_callback=(
#        [irods_types.KeyValPair,
#         lambda x: [ "key {} value {} ".format (x.key[i],x.value[i]) for i in range(x.len) ]
#        ], 
#        [irods_types.char_array,
#         lambda x: [ "strvalue {!s}".format (x) ]
#        ])
#    )
#    c.writeLine( "serverLog", out.getvalue() )

def _resolve_docker_method (cliHandle, attrNames):

    my_object = cliHandle
    if isinstance (attrNames, (str,bytes)):
        attrNames=attrNames.split('.')
    while my_object and attrNames:
        name = attrNames.pop(0)
        my_object = getattr(my_object,name,None)
    return my_object


def handle_docker_call(rule_args, callback, rei):

    docker_cmd = rule_args[0]
    arg = rule_args[1]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import docker
    docker_method = _resolve_docker_method (docker.from_env(), docker_cmd )
    docker_method (arg)

#
#def writeStringToCharArray(s, char_array):
#    for i in range(0, len(s)):
#        char_array[i] = s[i]
#
#def pythonRuleEnginePluginTest(rule_args, callback, rei):
#    with open('/tmp/from_core_py.txt', 'a') as f:
#        f.write(str(datetime.datetime.now()))
#        f.write('\n')
#        c = 0
#        for arg in rule_args:
#            f.write('\t')
#            f.write(str(c))
#            f.write(' : ')
#            f.write(str(arg))
#            f.write('\n')
#            c = c +1
#    callback.writeLine('serverLog', 'Printed to server log from python rule engine')


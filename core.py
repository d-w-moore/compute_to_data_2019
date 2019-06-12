import datetime
import json
from genquery import AS_LIST, AS_DICT, row_iterator
import warnings
from textwrap import dedent

from bytes_unicode_mapper import ( map_strings_recursively, to_bytes, to_unicode )

from checkperms import ( user_id_for_name, 
                         check_perms_on_data_object,
                         check_perms_on_collection   )


def logger (callback,strm='serverLog'):
    return  lambda s: callback.writeLine( strm, s )


def delayed_container_launch(rule_args, callback, rei):   # -- illustration only ?

    (container_command, container_config ) = rule_args

    p = logger(callback,'stdout')
    input_dir =  ""

    remote_host = ""

    for x in row_iterator ('RESC_LOC',"RESC_NAME = '{}'".format(dest_resc),AS_LIST,callback):
        remote_host = x[0]

    p('inputdir={!r}'.format(input_dir))
    
    output_dir = ""

    x = dedent("""\
               remote ("{remote_host}","") {
                   container_dispatch ( "{container_command}","{container_config}" )
               }
               """)
    callback.writeLine("stdout",x)
    callback.delayExec("<PLUSET>10s</PLUSET>", x ,"")

## -------------- auxiliary --------------

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


def _resolve_docker_method (cliHandle, attrNames):

    my_object = cliHandle

    if isinstance (attrNames, (str,bytes)):
        attrNames=attrNames.split('.')

    while my_object and attrNames:
        name = attrNames.pop(0)
        my_object = getattr(my_object,name,None)

    return my_object

def container_dispatch(rule_args, callback, rei):

    ( docker_cmd,
      config_file ) = rule_args

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import docker

    kw = {}

    if type(keywords_file) is str and keywords_file:
        config_json = readobj (callback, keywords_file )
        kw = map_strings_recursively( json.loads(config_json), to_bytes('utf8'))

    docker_method = _resolve_docker_method (docker.from_env(), docker_cmd, **kw )
    docker_method (args)


##def pep_api_data_obj_repl_post(rule_args,callback,rei ):
##  dataobjinp = rule_args[2]
##  cI = dataobjinp.condInput; condInp = { str(cI.key[i]):str(cI.value[i]) for i in range(cI.len) }
##  dest_resc = str( condInp['destRescName'] )
##  obj_path = str( dataobjinp.objPath )
##
### -- DEBUG VERSION
###
###from myinspect import myInspect
###
###def pep_api_data_obj_repl_post(a,c,r ):
###    import cStringIO
###    out=cStringIO.StringIO() 
###    myInspect ( a, stream=out ,types_callback=(
###        [irods_types.KeyValPair,
###         lambda x: [ "key {} value {} ".format (x.key[i],x.value[i]) for i in range(x.len) ]
###        ], 
###        [irods_types.char_array,
###         lambda x: [ "strvalue {!s}".format (x) ]
###        ])
###    )
###    c.writeLine( "serverLog", out.getvalue() )


import json
from genquery import AS_LIST, AS_DICT, row_iterator
import warnings
from textwrap import dedent as _dedent

from bytes_unicode_mapper import( map_strings_recursively as _map_strings_recursively,
                                  to_bytes as _to_bytes,
                                  to_unicode as _to_unicode)

from checkpoints import *


def get_object_size(callback, path):

    rv = callback.msiObjStat( path , 0)

    size = 0
    if  rv['status' ] and rv['code'] == 0:
        size = int(rv['arguments'][1].objSize)

    return str(size)


def readobj(callback, name):

    rv = callback.msiDataObjOpen (  "objPath={0}".format(name), 0 )

    returnbuffer = None
    desc = None

    if rv['status'] and rv['code'] >= 0: 
        desc = rv['arguments'][1]

    if type(desc) is int:
        siz = get_object_size (callback,name)
        rv = callback.msiDataObjRead ( desc, siz, 0 )
        returnbuffer = rv ['arguments'][2]

    return str(returnbuffer.buf)[:int(siz)] if returnbuffer else ""

## ------------------------------------------------

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

    if type(config_file) is str and config_file:
        config_json = read_data_object (callback, keywords_file )
        kw = _map_strings_recursively( json.loads(config_json), _to_bytes('utf8'))

    docker_method = _resolve_docker_method (docker.from_env(), docker_cmd, **kw )
    docker_method (args)


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def _delayed_container_launch(rule_args, callback, rei): 

    (container_command, container_config ) = rule_args

    p = make_logger(callback,'stdout')
    input_dir =  ""

    remote_host = ""

    for x in row_iterator ('RESC_LOC',"RESC_NAME = '{}'".format(dest_resc),AS_LIST,callback):
        remote_host = x[0]

    p('inputdir={!r}'.format(input_dir))
    
    output_dir = ""

    x = _dedent("""\
                remote ("{remote_host}","") {
                    container_dispatch ( "{container_command}","{container_config}" )
                }
                """)
    callback.writeLine("stdout",x)
    callback.delayExec("<PLUSET>1s</PLUSET>", x ,"")

#def pep_api_data_obj_repl_post(rule_args,callback,rei ):
#  dataobjinp = rule_args[2]
#  cI = dataobjinp.condInput; condInp = { str(cI.key[i]):str(cI.value[i]) for i in range(cI.len) }
#  dest_resc = str( condInp['destRescName'] )
#  obj_path = str( dataobjinp.objPath )
#

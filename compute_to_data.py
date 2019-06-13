
import json, uuid
from genquery import AS_LIST, AS_DICT, row_iterator
import irods_types
import warnings
from textwrap import dedent as _dedent

from bytes_unicode_mapper import( map_strings_recursively as _map_strings_recursively,
                                  to_bytes as _to_bytes,
                                  to_unicode as _to_unicode)

from checkpoints import *

## ------------------------------------------------

def _get_object_size(callback, path):

    rv = callback.msiObjStat( path , 0)

    size = 0
    if  rv['status' ] and rv['code'] == 0:
        size = int(rv['arguments'][1].objSize)

    return str(size)


def _read_data_object(callback, name):

    rv = callback.msiDataObjOpen (  "objPath={0}".format(name), 0 )

    returnbuffer = None
    desc = None

    if rv['status'] and rv['code'] >= 0:
        desc = rv['arguments'][1]

    if type(desc) is int:
        size = _get_object_size (callback,name)
        rv = callback.msiDataObjRead ( desc, size, 0 )
        returnbuffer = rv ['arguments'][2]

    return str(returnbuffer.buf)[:int(size)] if returnbuffer else ""

## ------------------------------------------------

def _resolve_docker_method (cliHandle, attrNames):

    my_object = cliHandle

    if isinstance (attrNames, (str,bytes)):
        attrNames=attrNames.split('.')

    while my_object and attrNames:
        name = attrNames.pop(0)
        my_object = getattr(my_object,name,None)

    return my_object


def _vet_acceptable_container_params (container_command , container_cfg, logger):

    if container_cfg["type"] != "docker" :
        logger("Choice of container technology now limited to Docker")
        return False

    acceptable_commands = [ "containers.run", "images.pull" ]

    if container_command not in acceptable_commands:
        logger("Docker API command must be one of: {0!r}" , acceptable_commands)
        return False

    return True


# =========================================


Metadata_Tag = "irods::compute_to_data_task"

#def #compute_to_data__

def meta_stamp_R (arg,callback, rei): meta_stamp (callback,arg[0],task_id='task-id')

def meta_stamp (callback, object_path, object_type = "-d", task_id = "" ):
    METADATA_TAG = Metadata_Tag
    rv = callback.msiString2KeyValPair("{METADATA_TAG}={task_id}".format(**locals()),
                                       irods_types.KeyValPair())
    if rv ['status']:
        rv = callback.msiSetKeyValuePairsToObj(rv['arguments'][1], object_path, object_type )

    return rv['status']


def container_dispatch(rule_args, callback, rei):

    ( docker_cmd,
      config_file,
      resc_for_data,
      output_locator,
      task_id        ) = rule_args

    logger = make_logger ( callback , "serverLog" )

    if not (resc_for_data != "" and this_host_tied_to_resc(callback, resc_for_data)) :
        logger("Input/output data objects must be located on a local resource"); return

    if not task_id: task_id = uuid.uuid1()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import docker

    if type(config_file) is str and config_file:
        config_json = _read_data_object (callback, config_file )
        kw = _map_strings_recursively( json.loads(config_json), _to_bytes('utf8'))

    if not _vet_acceptable_container_params( docker_cmd, config_json["container"] , logger) :  return

    docker_args = [ config_json["container"]["image"],
                  ]
    docker_opts = {}

    if docker_args[0] == "containers.run":

        docker_opts ['detach'] = False

        src_colln = config_json["external"]["src_collection"]
        if not user_has_access( callback, rei, "", "write", collection_path = src_colln):
            logger("Calling user must have write access on source"); return

        dst_colln = config_json["external"]["dst_collection"]
        if not user_has_access( callback, rei, "", "own", collection_path = dst_colln):
            logger("Calling user must have owner access on destination collection"); return

        eligible_inputs = [ "{COLL_NAME}/{DATA_NAME}".format(**d) for d in \
                            row_iterator( ["COLL_NAME","DATA_NAME"],
                                          "COLL_NAME = '{src_colln}' and META_DATA_ATTR_NAME != '{Metadata_Tag}'".format(**locals()),
                            AS_DICT,callback )
        ][:1]

        # calculate vault paths

        this_input = None


        if eligible_inputs:

            this_input = eligible_inputs[0]

            vault_paths = {}

            input_vault_info = {}
            input_leading_path = data_object_physical_path_in_vault( callback, this_input, resc_for_data, input_vault_info )
            if input_leading_path :
                rel_path = input_vault_info.get("vault_relative_path")
                if rel_path : vault_paths['input'] = "/".join(config_json["internal"]["src_directory"],rel_path)

            output_vault_info = {}
            output_leading_path = data_object_physical_path_in_vault( callback, output_locator, resc_for_data, output_vault_info )
            if output_leading_path:
                rel_path = output_vault_info.get("vault_relative_path")
                if rel_path : vault_paths['output'] = "/".join(config_json["internal"]["src_directory"],rel_path)

            if vault_paths:
                docker_opts [ 'volumes' ]  = {}
                if vault_paths.get('input'):
                    docker_opts ['volumes'][input_leading_path] = { 'bind': os.dirname(vault_paths['input']), 'mode': 'ro' }
                if vault_paths.get ('output'):
                    docker_opts ['volumes'][output_leading_path] = { 'bind': os.dirname(vault_paths['output']), 'mode': 'rw' }

    docker_method = _resolve_docker_method (docker.from_env(), docker_cmd  )

    docker_method (*docker_args, **docker_opts)


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# def _delayed_container_launch(rule_args, callback, rei):
#
#     (container_command, container_config ) = rule_args
#
#     p = make_logger(callback,'stdout')
#     input_dir =  ""
#
#     remote_host = ""
#
#     for x in row_iterator ('RESC_LOC',"RESC_NAME = '{}'".format(dest_resc),AS_LIST,callback):
#         remote_host = x[0]
#
#     p('inputdir={!r}'.format(input_dir))
#
#     output_dir = ""
#
#     x = _dedent("""\
#                 remote ("{remote_host}","") {
#                     container_dispatch ( "{container_command}","{container_config}" )
#                 }
#                 """)
#     callback.writeLine("stdout",x)
#     callback.delayExec("<PLUSET>1s</PLUSET>", x ,"")
#
# def pep_api_data_obj_repl_post(rule_args,callback,rei ):
#     dataobjinp = rule_args[2]
#     cI = dataobjinp.condInput; condInp = { str(cI.key[i]):str(cI.value[i]) for i in range(cI.len) }
#     dest_resc = str( condInp['destRescName'] )
#     obj_path = str( dataobjinp.objPath )

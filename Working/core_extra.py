import datetime
import json
from genquery import *
import session_vars
import uuid

def writeStringToCharArray(s, char_array):
    for i in range(0, len(s)):
        char_array[i] = s[i]

def register_object_via_admin_proxy(rule_args, callback, rei):
    logPath = rule_args[0]
    rescN = rule_args[1]
    phyPath = rule_args[2]
    targetType = rule_args[3] # "collection" [ or "null" for dataObj ]
    rv = callback.msiregister_as_admin(logPath,rescN,phyPath,targetType,0)
    statusOut = rv['arguments'][4]
    code = rv['code']
    callback.writeLine("stdout","code {} , status out parm {}".format(code,statusOut))

#-----------------------------------------------------------------------

def split_irods_path( s ):
    s.split("/")
    return s[:-1],s[-1]

def remote_container_execute (rule_args,callback,rei):
    container_run_jsonConfigName = rule_args [0]
    collname, dataname = split_irods_path( container_run_jsonConfigName)
    configName = ""

    try:
        fileNameEntries = list(row_iterator("COLL_NAME,DATA_NAME", 
                                     "COLL_NAME = '{}' and DATA_NAME = '{}'".format(collname,dataname) , AS_DICT, callback))
        if len(fileNameEntries) == 1:
            configName = "{COLL_NAME}/{DATA_NAME}".format(**fileNameEntries[0])
    except: pass

    if configName:
        with open(configName,'r') as f:
            open('/tmp/configthing','w').write(f.read())

def asyncRemoteExecute(rule_args, callback, rei):

    jsonPayload =  rule_args[0]
    job_uuid = rule_args[1]
    parms_Extra = rule_args[2]
    
    mylist = parms_Extra.split("/")
    params_ = { 'REMOTE-HOST':mylist[0] , 'STRING-TO-PRINT':mylist[1] }

    # -- parse the JSON client-side to extract needed details

    executeConfig = json.loads(jsonPayload)

    # -- generate UUID if none provided

    job_uuid = rule_args[1]
    if not job_uuid:
        job_uuid = str(uuid.uuid1()) 
        rule_args[1] = job_uuid

    computeStorageResc = executeConfig["compute_storage_resc"]
    computeHost = ""
    for h in row_iterator("RESC_LOC","RESC_NAME='{}'".format(computeStorageResc)):
        computeHost = h["RESC_LOC"]

    logical_config_path = storeJsonPayload( callback, rei, job_uuid, computeStorageResc, jsonPayload )
    callback.writeLine("stdout",'logical config path = '+logical_config_path)

    x= """
       remote ("%(REMOTE-HOST)s","") {
           *a="%(STRING-TO-PRINT)s"
           pythonRuleEnginePluginTest(*a," [*a]")
       }
       """ % params_ 
    callback.writeLine("stdout",x)
    callback.delayExec("<PLUSET>10s</PLUSET>", x ,"")

#============================================================

def storeJsonPayload(cbk, rei, jobUUID='', computeResource='', jsonString=None ):

    map_ = session_vars.get_map(rei) ['client_user']
    colln = '/{irods_zone}/home/{user_name}'.format(**map_)

    if jobUUID:
        colln += "/{}".format(jobUUID)
        retv = cbk.msiCollCreate (colln,"0",0)
        cbk.writeLine('stdout','coll_create -> {}'.format(retv['code']))

    cbk.writeLine('stdout', 'coln = {} '.format(colln))

    config_file_name  = ""

    if jsonString is not None:

        descriptor = ""

        config_file_name = colln + "/config.json"
        try:

            create_rv = cbk.msiDataObjCreate( config_file_name, "forceFlag=++++destRescName={}".format(computeResource), 0)
            descriptor = create_rv ['arguments'][2]

        except:
            config_file_name = ""

        if type(descriptor) is not int:

            pass
            cbk.writeLine("serverLog", "Could not create JSON data object")
        else:

            try:

                write_rv = cbk.msiDataObjWrite( descriptor , jsonString, 0 )
                bytesWritten = write_rv['arguments'][2]
                cbk.writeLine("stdout","{} bytes written".format(bytesWritten))

                close_rv = cbk.msiDataObjClose( descriptor, 0 )
                cbk.writeLine("stdout","descriptor {} closed -> {}".format(descriptor,close_rv['arguments'][1]))

            except: pass

        return config_file_name

#   check perms on input
#
#       - collections 
#
#   make data objects available
#       
#       -> set ACL's for client user
#       -> replicate to compute resource

#------------------------------- REPLICATE_DATA_OBJECTS ( from_coll , to_resc , trim_from_resc ) ----------------------------

from genquery import *

def split_irods_path( s ):
    t = s.split("/")
    return "/".join(t[:-1]),t[-1]

def replicate_data_objects( rule_args , callback , rei):

    from_object = rule_args[0]
    to_resource = rule_args[1]
    from_resource = rule_args[2]
    trim_after_replication = rule_args[3]

    if len(to_resource) == 0 or len(from_object) == 0: return

    path,obj = split_irods_path (from_object)

    condition = "COLL_NAME = '{0}' and DATA_NAME = '{1}' ".format(path,obj)
    if from_resource:
        condition += " and DATA_RESC_NAME = '{}' ".format(from_resource)

    data_objects = list(row_iterator('DATA_NAME,COLL_NAME,DATA_RESC_NAME,DATA_REPL_NUM', condition, AS_DICT, callback))

#   callback.writeLine("stderr", " ** condition:" + pprint.pformat(condition))
#   callback.writeLine("stderr", " ** data_objects: " + pprint.pformat(data_objects))

    if not(data_objects):
        condition = "COLL_NAME = '{0}' || like '{0}/%' " .format (from_object)
        if from_resource:
            condition += " and DATA_RESC_NAME = '{}'".format(from_resource)
        data_objects = list(row_iterator('DATA_NAME,COLL_NAME,DATA_RESC_NAME,DATA_REPL_NUM', condition, AS_DICT, callback))

    replicated = {}

    for dobj in data_objects:

        full_path = "{COLL_NAME}/{DATA_NAME}".format(**dobj)
        if dobj['DATA_RESC_NAME'] == to_resource:
            replicated[full_path] = True
        else:
            old_replication_status = replicated.get(full_path, False)

            if not old_replication_status:
                #callback.writeLine("stderr", "replicating: \n" + pprint.pformat(dobj))
                retval = callback.msiDataObjRepl( full_path, "destRescName={0}".format(to_resource),0)
                new_replication_status = retval['status']
                replicated [full_path] = new_replication_status
         
            if new_replication_status and not(old_replication_status) and trim_after_replication and \
             dobj['DATA_RESC_NAME'] == from_resource != "":

                trim_retval = callback.msiDataObjTrim( "{COLL_NAME}/{DATA_NAME}".format(**dobj), "null",
                                                       dobj['DATA_REPL_NUM'], "1", "null", 0)

#-----------------------------------------------------------------------------------------------------------------------------


import json
import pprint
import session_vars
import uuid

def storeJsonPayload(c, r, jobUUID='', jsonString=None ):

    map_ = session_vars.get_map(r) ['client_user']
    colln = '/{irods_zone}/home/{user_name}'.format(**map_)

    if jobUUID:
        colln += "/{}".format(jobUUID)
        retv = c.msiCollCreate (colln,"0",0)
        c.writeLine('stdout','coll_create -> {}'.format(retv['code']))

    c.writeLine('stdout', 'coln = {} '.format(colln))

    if jsonString is not None:

        descriptor = ""

        try:

            create_rv = c.msiDataObjCreate( colln + "/config.json", "forceFlag=", 0)
            descriptor = create_rv ['arguments'][2]

        except: pass

        if type(descriptor) is not int:

            pass
            c.writeLine("serverLog", "Could not create JSON data object")
        else:

            try:

                write_rv = c.msiDataObjWrite( descriptor , jsonString, 0 )
                bytesWritten = write_rv['arguments'][2]
                c.writeLine("stdout","{} bytes written".format(bytesWritten))

                close_rv = c.msiDataObjClose( descriptor, 0 )
                c.writeLine("stdout","descriptor {} closed -> {}".format(descriptor,close_rv['arguments'][1]))

            except: pass


#=============================

def asyncRemoteExecute(rule_args, callback, rei):

    jsonPayload =  rule_args[0]
    job_uuid = rule_args[1]
    parms_Extra = rule_args[2]
    
    mylist = parms_Extra.split("/")
    params_ = { 'REMOTE-HOST':mylist[0] , 'STRING-TO-PRINT':mylist[1] }

    executeConfig = json.loads(jsonPayload)

    # -- generate UUID if none provided

    job_uuid = rule_args[1]
    if not job_uuid:
        job_uuid = str(uuid.uuid1()) 
        rule_args[1] = job_uuid

    storeJsonPayload(callback,rei,job_uuid)

    x=                 """
                       remote ("%(REMOTE-HOST)s","") {
                           *a="%(STRING-TO-PRINT)s"
                           pythonRuleEnginePluginTest(*a," [*a]")
                       }
                       """ % params_ 

    callback.writeLine("stdout",x)
#   callback.delayExec("<PLUSET>10s</PLUSET>", x ,"")



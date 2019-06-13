from genquery  import row_iterator, AS_DICT, AS_LIST
import session_vars

## --------------- utility functions --------------

def make_logger (callback,strm='serverLog'):
    return  lambda s: callback.writeLine( strm, s )

def create_collection (args,callback,rei):
    objpath=args[0]
    rv = callback.msiCollCreate (objpath, "0", 0)
    
def set_acl_inherit (args,callback,rei):
    objpath = args[0]
    user = args[1]
    rv = callback.msiSetACL ("recursive", "admin:inherit", user, objpath)

def set_acl_own (args,callback,rei):
    pr = make_logger (callback)
    objpath = args[0]
    user = args[1]
    rv = callback.msiSetACL ("default", "admin:own", user, objpath)

#==================
#    utility
#==================

def split_irods_path( s ):
    elem = s.split("/")
    return "/".join(elem[:-1]),elem[-1]

def this_host_tied_to_resc(callback, resc ):
    import socket
    this_host = socket.gethostname()
    tied_host = ""
    for rescvault in row_iterator( 'RESC_LOC',"RESC_NAME = '{resc}'".format(**locals()), AS_DICT,callback):
        tied_host = rescvault['RESC_LOC']
    return this_host == tied_host


def data_object_physical_path_in_vault_R(args, callback, rei):  # - test function (call as rule)
    pr = make_logger(callback,'stdout')
    v_retn = {}
    phy = data_object_physical_path_in_vault (callback, *args[:3] , vault_validate = v_retn)
    pr('phy = %r'%phy)
    pr('v_retn = %r'%v_retn)


# Find (opt. force instantiation of) a data object path on the desired [resc] .
# In the case of an output directory, we'll have to create a dummy data object because collection's
#  corresponding directories do not autovivify in a vault without a data replica to drive the process.
# ---
# NB this function does not create the collection itself; it is assumed to have been created already.

def data_object_physical_path_in_vault(callback, path, resc, force_creation_on_resc, vault_validate = None):

    #pr = make_logger(callback,'stderr')

    colln, dataobj = split_irods_path( path )

    status = _data_object_exists_targeting_resc( callback, resc, colln, dataobj )

    if status != 'already-exists' and force_creation_on_resc:
        close_rv = {} ; repl_rv  = {} ; close_rv = {}
        if (status == 'need-repl'):
            repl_rv = callback.msiDataObjRepl(path, "destRescName={}".format(resc), 0)
            #pr("repl  status = %r arg-retn %r " % (repl_rv['status'],repl_rv['arguments'][2]))
        else:
            create_rv = callback.msiDataObjCreate(path, "forceFlag=++++destRescName={}".format(resc), 0)
            descriptor = create_rv['arguments'][2]
            if type(descriptor) is int and descriptor > 0:
                close_rv =  callback.msiDataObjClose(descriptor,0)
                #pr("close(%r) status = %r arg-retn %r " % (descriptor,close_rv['status'],close_rv['arguments'][1]))
    v = {}

    if type(vault_validate) is dict:    # - get vault path to match against data object phys. path
        v = vault_validate
        leading_path = ""
        for rescvault in row_iterator( 'RESC_VAULT_PATH',"RESC_NAME = '{resc}'".format(**locals()), AS_DICT,callback ):
            leading_path = rescvault['RESC_VAULT_PATH']

    phys_path = ''
    for p in row_iterator("DATA_PATH",
                          "DATA_RESC_NAME = '{resc}' and DATA_NAME = '{dataobj}' and COLL_NAME = '{colln}' ".format(**locals()),
                          AS_DICT,callback):
        phys_path = p['DATA_PATH']

    if leading_path != '' and phys_path.startswith( leading_path ):
        v['vault_relative_path'] = phys_path [len(leading_path):].lstrip('/')

    return leading_path

#==================
# test and support
#  for the above function
#==================

def data_object_exists_targeting_resc_R( rule_args, callback, rei):
    pr = make_logger(callback,'stdout')
    pr ( "data obj exists = %r" %  data_object_exists_on_resc(callback, *rule_args[:3]) )

def _data_object_exists_targeting_resc( callback, resc, coll, base_data_name ):
    rescs_having_obj = [ col['RESC_NAME'] for col in row_iterator ( "RESC_NAME" , "COLL_NAME = '{}' and DATA_NAME = '{}'".format(coll,base_data_name),
                                                                    AS_DICT, callback)]
    if len( rescs_having_obj ) > 0 :
        return 'need-repl' if (resc not in rescs_having_obj) else 'already-exists'
    return '' # - did not find  data object on any resc

#==================
# other necessaries
#==================

def user_id_for_name(callback, username):

    user_id=""
    for i in row_iterator( 'USER_ID,USER_NAME', "USER_NAME = '{}'".format(username), AS_DICT, callback):
        if 0 == len(user_id):
          user_id = i['USER_ID']
    return user_id

def get_user_name (callback,rei):
    u = ''
    try:
        u = session_vars.get_map(rei)['client_user']['user_name']
    except: pass
    return u
    
def user_has_access_R(rule_args, callback, rei ): 

    pr = make_logger(callback,'stdout')
    username = rule_args[0]
    if username == "" : username = get_user_name(callback,rei)

    if username != "" :
        access_type_name = rule_args[1]
        datobj = rule_args[2]
        colln = rule_args[3]
        a = user_has_access  (callback, rei, username, access_type_name, data_object_path=datobj, collection_path=colln)
        pr ('access = ' + repr(a))
    else:
        pr ("username could not be determined")

def user_has_access (callback, rei, username, access_type_name, data_object_path='', collection_path=''):

    access = False
    access_types = { 'write':'1120', 'read':'1050', 'own':'1200' }
    user_ID = user_id_for_name (callback,username)

    do_query = (user_ID != '')

    if data_object_path and not collection_path :

        coll_name , data_name = split_irods_path (data_object_path)

        condition = "DATA_NAME = '{0}' and  COLL_NAME = '{1}' "\
                    "and DATA_ACCESS_USER_ID = '{2}' and DATA_ACCESS_TYPE >= '{3}'".format(
                    data_name, coll_name, user_ID, access_types[access_type_name] )

    elif collection_path and not data_object_path:

        condition = "COLL_NAME = '{0}' and COLL_ACCESS_USER_ID = '{1}' and COLL_ACCESS_TYPE >= '{2}' ".format(
                    collection_path, user_ID, access_types[access_type_name] )

    else :
        do_query = False

    if do_query: 
        for i in row_iterator( "COLL_NAME", condition, AS_LIST, callback):
            access = True

    return access


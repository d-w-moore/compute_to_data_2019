from genquery  import row_iterator, AS_DICT

def split_irods_path( s ):
    elem = s.split("/")
    return "/".join(elem[:-1]),elem[-1]

def this_host_tied_to_resc(callback, resc ):
    import socket
    this_host = socket.gethostname()
#   callback.writeLine("stderr"," host = '{}'".format(this_host))
    tied_host = ""
    for rescvault in row_iterator( 'RESC_LOC',"RESC_NAME = '{resc}'".format(**locals()), AS_DICT,callback):
        tied_host = rescvault['RESC_LOC']
#   callback.writeLine("stderr", "tied = %r"%tied_host)
    return this_host == tied_host


def data_object_physical_path_in_vault(callback, path, resc, vault_validate = None):

    v = {}

    if type(vault_validate) is dict:
        v = vault_validate
        leading_path = ""
        for rescvault in row_iterator( 'RESC_VAULT_PATH',"RESC_NAME = '{resc}'".format(**locals()), AS_DICT,callback ):
            leading_path = rescvault['RESC_VAULT_PATH']
        v ['is_in_vault'] = (leading_path != "")


    # - get data object's absolute physical data path

    colln, dataobj = split_irods_path( path )

    phys_path = ''

    for p in row_iterator("DATA_PATH",
     "DATA_NAME = '{dataobj}' and COLL_NAME = '{colln}' ".format(**locals()),
     AS_DICT,callback):

        phys_path = p['DATA_PATH']

    if v['is_in_vault'] and phys_path . startswith( leading_path ):
        v['vault_relative_path'] = phys_path [len(leading_path):].lstrip('/')

    return phys_path


def user_id_for_name(rule_args, callback, rei):

    user_id= ''
    user_name = rule_args[0]
    for i in row_iterator( 'USER_ID,USER_NAME', "USER_NAME = '{}'".format(user_name), AS_DICT, callback):
        if 0 == len(user_id):
          user_id = i['USER_ID']
    if 0 < len(user_id) : rule_args[1] = user_id


def check_perms_on_data_object(rule_args, callback, rei):
    access_types = { 'write':'1120', 'read':'1050', 'own':'1200' }
    logical_path = rule_args[0]
    user_id = rule_args[1]
    access_type_name = rule_args[2]
    access = False
    data_path_elements = logical_path.split("/")
    coll_name = "/".join( data_path_elements[:-1] )
    data_name = data_path_elements[-1]
    condition = "DATA_NAME = '{0}' and  COLL_NAME = '{1}' "\
                "and DATA_ACCESS_USER_ID = '{2}' and DATA_ACCESS_TYPE >= '{3}'".format(
                data_name, coll_name, user_id, access_types[access_type_name] )
    physical_path=""
    host_name = ''
    for i in row_iterator( "RESC_LOC,DATA_NAME,DATA_PATH,DATA_ACCESS_TYPE", condition, AS_DICT, callback):
        host_name = i['RESC_LOC']
        physical_path  = i['DATA_PATH']
    if host_name:
        rule_args[3] = host_name
        rule_args[4] = physical_path

def check_perms_on_collection(rule_args, callback, rei):
    access_types = { 'write':'1120', 'read':'1050', 'own':'1200' }
    logical_path = rule_args[0]
    user_id = rule_args[1]
    access_type_name = rule_args[2]
    access = False
    condition = "COLL_NAME = '{0}' and COLL_ACCESS_USER_ID = '{1}' and COLL_ACCESS_TYPE >= '{2}' ".format(
                logical_path, user_id, access_types[access_type_name] )
    for i in row_iterator( "COLL_NAME,COLL_ACCESS_TYPE", condition, AS_DICT, callback): access = True
    rule_args[3] = "1" if access else ""


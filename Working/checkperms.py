
def user_id_for_name(rule_args, callback, rei):

    user_id= ''
    user_name = rule_args[0]
    for i in row_iterator( 'USER_ID,USER_NAME', "USER_NAME = '{}'".format(user_name), AS_DICT, callback):
        if 0 == len(user_id):
          user_id = i['USER_ID']
    if 0 < len(user_id) : rule_args[1] = user_id

def check_perm_on_datobj(rule_args, callback, rei):
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

def check_perm_on_colln(rule_args, callback, rei):
    access_types = { 'write':'1120', 'read':'1050', 'own':'1200' }
    logical_path = rule_args[0]
    user_id = rule_args[1]
    access_type_name = rule_args[2]
    access = False
    condition = "COLL_NAME = '{0}' and COLL_ACCESS_USER_ID = '{1}' and COLL_ACCESS_TYPE >= '{2}' ".format(
                logical_path, user_id, access_types[access_type_name] )
    for i in row_iterator( "COLL_NAME,COLL_ACCESS_TYPE", condition, AS_DICT, callback): access = True
    rule_args[3] = "1" if access else ""


def set_acl_inherit (args,callback,rei):
    objpath = args[0]
    user = args[1]
    rv = callback.msiSetACL ("recursive", "admin:inherit", user, objpath)

def set_acl_level (args,callback,rei):
    #pr = make_logger (callback)
    objpath = args[0]
    user = args[1]
    level = args[2]
    rv = callback.msiSetACL ("default", "admin:{}".format(level), user, objpath)

def set_acl_own (args,callback,rei):
    #pr = make_logger (callback)
    objpath = args[0]
    user = args[1]
    rv = callback.msiSetACL ("default", "admin:own", user, objpath)


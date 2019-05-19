import datetime
import json
import pprint
import uuid

def writeStringToCharArray(s, char_array):
    for i in range(0, len(s)):
        char_array[i] = s[i]

def pythonRuleEnginePluginTest(rule_args, callback, rei):
    with open('/tmp/from_core_py.txt', 'a') as f:
        f.write(str(datetime.datetime.now()))
        f.write('\n')
        c = 0
        for arg in rule_args:
            f.write('\t')
            f.write(str(c))
            f.write(' : ')
            f.write(str(arg))
            f.write('\n')
            c = c +1
    callback.writeLine('serverLog', 'Printed to server log from python rule engine')

def get_size(a,callback,rei):
    callback.writeLine("stdout", get_object_size(callback, a[0] ))
    pass

def get_object_size(callback, path):

    rv = callback.msiObjStat( path , 0)

    size = 0
    if  rv['status' ] and rv['code'] == 0:
        size = int(rv['arguments'][1].objSize)

    return str(size)


def asyncRemoteExecute(rule_args, callback, rei):

    job_params_object = rule_args[0]

    contents = readobj(callback, job_params_object)
    callback.writeLine("stderr","len = "+str(len(contents)))
    #callback.writeLine( "stdout", ">>>>>>>>>>>>>\n{}\n<<<<<<<<<<<<<".format(contents))
    j = json.loads(str( contents))
    callback.writeLine("stdout", ">>> {} <<<" .format(pprint.pformat(j)) )

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

    '''
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
    callback.delayExec("<PLUSET>10s</PLUSET>", x ,"")
    '''



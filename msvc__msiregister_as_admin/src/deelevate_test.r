mainrule
{
    foreach (*x in split(*funcs,",")) 
    {
        register_colln_or_dataobj(*x)
    }
}

register_colln_or_dataobj(*strg)
{
    # - storage resource for registration can be defaulted ("") or overridden ("rescname")
    if (*rescN == "") {
        foreach (*r in select RESC_NAME,RESC_VAULT_PATH ) {
          *vaultPattern= *r.RESC_VAULT_PATH ++ "/*" #writeLine("stdout","vaultPattern = *vaultPattern")
          if (*phyPath like *vaultPattern) { *rescN = *r.RESC_NAME }
        }
        writeLine("stdout","rescN => [*rescN]");
    }
    # - storage resource not provided or detected, do not proceed
    if (*rescN != "") {
        if (*strg == "c") {   # -- collection
            *type="collection"
            *xx=msiregister_as_admin(*logPath,*rescN,*phyPath,*type,*yy)
            writeLine("stdout"," coln [*xx] [*yy]")
        } else if (*strg == "d") { # -- data object
            *type="null"
            ## *xx=msiregister_as_admin(*logPath,*rescN,*phyPath,*type,*yy)
            *xx=msiregister_as_admin ("/tempZone/home/alice/aaa",*rescN,"/junk/irods/aaa",*type,*yy)
            writeLine("stdout","as_admin   null [*xx] [*yy]")
            *xx=msiPhyPathReg        ("/tempZone/home/alice/bbb",*rescN,"/junk/irods/bbb",*type,*yy)
            writeLine("stdout","PhyPathReg null [*xx] [*yy]")
        }
    }
}

input  *funcs=$"d,",*phyPath=$"",*logPath=$"",*rescN=$"demoResc"
output ruleExecOut

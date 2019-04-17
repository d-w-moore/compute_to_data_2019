test() {

      *Param1 = "string"
      msiregister_as_admin( *Param1, *Param2 )
      writeLine("stdout", *Param1)
      
}

INPUT  null
OUTPUT ruleExecOut

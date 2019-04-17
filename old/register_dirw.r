f {

# *type="collection"
# *path="/tempZone/home/alice/newdir"
# *resc="demoResc"
# *phy="/junk/misc/newdir"
# *xx=msiPhyPathReg(*path,*resc,*phy,*type,*yy)
# writeLine("stdout"," coln [*xx] [*yy]")

  # ==== #

  *type="null"
  *path="/tempZone/home/alice/newdir/file"
  *resc="demoResc"
  *phy="/junk/misc/newdir/file"
  *x=msiPhyPathReg(*path,*resc,*phy,*type,*y)
  writeLine("stdout"," null [*x] [*y]")

}

input  null
output ruleExecOut

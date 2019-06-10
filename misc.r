# Register a physical path (collection or data object)
#  with temporary admin privilege
#  (* see msvc_register_as_admin)
f {
# *path="/tempZone/home/rods/dude2"
  *resc="ubuntur16Resource"
# *phy="/var/lib/irods/Vault/home/rods/dude2"
# *type="collection"
  *y=-999999999
  *x=msiPhyPathReg(*path,*resc,*phy,*type,*y)
  writeLine("stdout","[*x][*y]")
}

input *path=$"/tempZone/home/rods/dude2",*phy=$"/var/lib/irods/Vault/home/rods/dude2",*type=$"collection"
output ruleExecOut


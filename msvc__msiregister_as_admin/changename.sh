#!/bin/sh
#git clone http://github.com/d-w-moore/demo_twoparams_msvc
#cd demo_twoparams_msvc/
mv src/lib-microservice-{register_as_admin,$1}.cpp  
x=$(find -type f|xargs grep demo_twoparams -l|grep -v "\.git\>")
perl -i.orig -pe "s/register_as_admin/$1/g" $x

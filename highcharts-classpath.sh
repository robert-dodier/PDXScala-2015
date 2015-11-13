j=`find /home/robert/by-others/github/quantifind/wisp/ -name wisp\*.jar`
l=`find /home/robert/by-others/github/quantifind/wisp/ -name \*.jar | sort -r`
export CLASSPATH=`echo $l | sed 's/ /:/g'`:$j:$CLASSPATH

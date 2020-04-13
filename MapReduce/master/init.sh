set -e

mkdir -p /tmp/tcesar
cd /tmp/tcesar
rm -f -r *
mkdir reduces
mkdir maps
mkdir shuffles
mkdir shufflesreceived
mkdir splits
scp /cal/homes/tcesar/MesDocuments/SLR205/MapReduce/master/machines.txt /tmp/tcesar/machines.txt
scp /cal/homes/tcesar/MesDocuments/SLR205/MapReduce/slave/Slave.jar /tmp/tcesar/Slave.jar

num="/cal/homes/tcesar/MesDocuments/SLR205/MapReduce/master/splits/$1.txt"
scp $num /tmp/tcesar/splits/test.txt

set -e

mkdir -p /tmp/tcesar
cd /tmp/tcesar
rm -f -r *
mkdir reduces
mkdir maps
mkdir shuffles
mkdir shufflesreceived
mkdir splits
range=$(( $2 - 1))
for i in $(seq 0 $range)
do
    mkdir shuffles/$i
done
scp /cal/homes/tcesar/MesDocuments/SLR207/MapReduce/master/zip.sh /tmp/tcesar/shuffles/zip.sh
scp /cal/homes/tcesar/MesDocuments/SLR207/MapReduce/master/unzip.sh /tmp/tcesar/shufflesreceived/unzip.sh
scp /cal/homes/tcesar/MesDocuments/SLR207/MapReduce/master/machines.txt /tmp/tcesar/machines.txt
scp /cal/homes/tcesar/MesDocuments/SLR207/MapReduce/slave/Slave.jar /tmp/tcesar/Slave.jar
scp /cal/homes/tcesar/MesDocuments/SLR207/MapReduce/master/send_reduces.sh /tmp/tcesar/send_reduces.sh
num="/cal/homes/tcesar/MesDocuments/SLR207/MapReduce/master/splits/$1.txt"
scp $num /tmp/tcesar/splits/test.txt

cd /tmp/tcesar/shufflesreceived
mkdir files
ls *.zip  >/dev/null 2>&1  || echo ok

for f in *.zip ; do
    unzip -qq -j $f -d "files/"
done
echo ok

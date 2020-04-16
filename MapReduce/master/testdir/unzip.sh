mkdir files
for f in *.zip ; do
    unzip -qq -j $f -d "files/"
done
echo ok

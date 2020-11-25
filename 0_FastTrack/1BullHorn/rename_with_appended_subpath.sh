#!/bin/bash -e

path=$(pwd)
from=$1
mkdir -p $path/$from"_upload" || true
to=$path/$from"_upload"

#ls -1 $(find $from -type f)

find $from -type f \( -iname *.pdf -o -iname *.doc -o -iname *.docx -o -iname *.xls -o -iname *.xlsx -o -iname *.rtf -o -iname *.htm -o -iname *.html -o -iname *.txt -o -iname *.png  -o -iname *.jpg -o -iname *.jpeg -o -iname *.gif -o -iname *.bmp -o -iname *.msg \) |
#find $from -type f |
    while IFS= read oldfilename; do
	#echo from $from to $to

	olddir=`echo $oldfilename | sed -r "s/(.+)\/.+/\1/"`
	echo -e "olddir \t\t= $olddir"

	echo -e "oldfilename \t= $oldfilename"

	filename=`echo $oldfilename | grep -oP "[^/]*$"`
	echo -e "filename \t= $filename"

	newfilename=`echo $oldfilename | cut -d '/' -f2- | sed "s:/:_:g"`
	echo -e "newfilename \t= $newfilename"

#	echo "mv $oldfilename $to/$newfilename"
	echo "mv $oldfilename $to/$newfilename"
	mv "$oldfilename" "$to/$newfilename" || true
	echo -e "\n"
   done

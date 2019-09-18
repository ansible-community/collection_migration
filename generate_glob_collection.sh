#!/bin/bash

set -eux

if [ -d $1 ]; then

	pullfrom=()
	pullfrom+=$(find $1/lib/ansible/plugins/* -maxdepth 1 -type d |grep -v __)
	#pullfrom+=("$1/lib/ansible/modules" "$1/lib/ansible/module_utils")

	files=()
	for t in ${pullfrom[@]}
	do
		files+=" "
		files+=$(find $t -type f|grep -v '.pyc'|grep -v '.pyo')
	done

	category=''
	echo 'glob:'
	for f in ${files}
	do
		f=${f#$1/lib/ansible/plugins/}
		f=${f#$1/lib/ansible/}

		cat=$( echo $f |cut -f1 -d'/')
		token=${f#"$cat/"}

		# only print on cat change, files should already be grouped by category
		if [ "$cat" != "$category" ]; then
			echo "  $cat:"
			category=$cat
		fi

		# naked init is skipped
		if [ "$token" != "__init__.py" ]; then
			echo "    - ${token}"
		fi
	done
fi

#/bin/bash
pypirc=$(cat << EOF
[distutils]
index-servers = $1

[$1]
repository = $2
username: $3
password: $4
EOF
)

pipconf=$(cat << EOF
[global]
index-url = https://pypi.org/simple
extra-index-url = https://$3:$4@$5
EOF
)

echo "$pypirc" > ~/.pypirc

mkdir -p ~/.config/pip/
echo "$pipconf" > ~/.config/pip/pip.conf

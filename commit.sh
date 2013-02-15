#!/bin/bash

echo -n "Enter branch name - "
read branch
echo -n "Enter commit message - "
read msg
git commit -am "$(echo "$msg")"
git checkout master
git merge $branch
git push
git checkout $branch

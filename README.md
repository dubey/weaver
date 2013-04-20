weaver
======

A distributed graph processing engine


Informal notes for installing:
need: 
g++-4.7 as default g++ compiler (g++ --version to check)

ayush branch of po6
e from around feb 2013
busybee from aug 29
in above three:
autoreconf -i
./configure
make
sudo make install

And put in ~/.bashrc the following two lines and restart bash
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
export LD_LIBRARY_PATH


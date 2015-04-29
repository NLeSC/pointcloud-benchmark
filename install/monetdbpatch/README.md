Patches
=======

The monetdb loaders and queriers use some functionalities that are not yet available 
(as of date April 2015) in the default repository branch.

In this folder you can find the patch to be applied that will add this functionalities.

To apply them use

cd MonetDBsrc/

hg update -r default

hg patch path/to/MortonContainsAnalyze.diff 

If an error is produced, try to manually add the missing lines

In case the hg patch does not return an error it will open a window to type a 
commit message and do a commit to save your patch.
You need to abort the commit in case it (the hg patch) succeeds so the local 
clone is no updated, i.e., your patch remains as a temporary version. 
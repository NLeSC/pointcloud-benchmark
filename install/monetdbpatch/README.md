Patches
=======

The monetdb loaders and queriers use some functionalities that are not yet available 
(as of date April 2015) in the default repository branch.

In this folder you can find two patches (one for the new contains method and 
another for the morton-related functions)

To apply them you can either make the changes as indicated in the files or use

hg import [patch file]

Then, you can do hg diff to see if the differences are the expected ones
Ubuntu 14.04 installation
=========================

-- NOT FINISHED --

These instructions can be used to install the required software in 
in a Ubuntu 14.04 system.

Some of these tools require custom builds of the software. 
We choose to install all the required software in /opt/sw

All the installed software must be added to PATH, LD_LIBRARY_PATH, C_PATH and PYTHONPATH. 
For that we use a file in /opt/sw/export.sh where all modification to environmental variables must be done 

Is is assumed all installations are done with root user (though most of them install in custom locations)

To prepare the installation directory and to guarantee that all installed 
software is available for all users:

`mkdir /opt/sw`

`touch /opt/sw/export.sh`

`echo "source /opt/sw/export.sh" > /etc/profile.d/custom.sh`

See `install_required` for the installation of the required dependencies.
See the different `install_optional_*` for the optional dependencies.

After all the installations give read/execute access to all users

`cd /opt/sw/`

`chown -R root:root *`

`chmod -R 755 *`

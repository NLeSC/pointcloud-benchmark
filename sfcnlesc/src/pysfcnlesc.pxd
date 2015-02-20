#distutils: language = c++
from libcpp.vector cimport vector
from libcpp.string cimport string

# This is the code at C++ layer
cdef extern from "sfcnlesc.h":
    vector[long unsigned int] getRangesWKT(string wkt, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels,  int mergeconsecutive, int maximumranges);

# This is the code in the Python layer
cpdef getranges(str wkt, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels,  int mergeconsecutive, int maximumranges)

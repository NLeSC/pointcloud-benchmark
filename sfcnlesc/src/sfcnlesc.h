#ifndef __SFCNLESC_H__
#define __SFCNLESC_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <geos_c.h>
#include <inttypes.h>
#include <vector>
#include <array>
#include <math.h>
#include <algorithm>
#include <cstring>
#include <ctype.h>
#include <sstream>
#include <iostream>
#include <fstream>

using namespace std;

vector<uint64_t> getRangesWKT(string wkt, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels, int mergeconsecutive, int maximumranges);

#endif

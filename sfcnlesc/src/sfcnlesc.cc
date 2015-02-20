/*
 ============================================================================
 Name        : sfcnlesc.cc
 Author      : Oscar Martinez Rubi
 Version     :
 Copyright   : 
 Description : 
 ============================================================================
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <geos_c.h>
#include <inttypes.h>
#include <vector>
#include <math.h>  
#include <algorithm>
#include <cstring>
#include <ctype.h>
#include <sstream>
#include <iostream>
#include <fstream>

#define __STDC_FORMAT_MACROS

// DEFINITIONS
#define MAXWKTLEN 1047551
#define MAXBITS 31
#define MAXDIGITS 15

// CONSTANTS
static const long ONELONG = 1; 
static const int NUMELEMQTC = 4;
static const int NUMLEVELFACTOR = 0;
//static const int MAXRANGES = 8;

using namespace std;

// GLOBAL VARIABLES
int numbits;
int numlevels;
int domain[4];
int startlevel;
uint64_t startquadcode;
uint32_t startquad[4];
int maxranges;

bool double_equals(double a, double b, double epsilon = 0.001)
{
    return abs(a - b) < epsilon;
}

void usage(char *me)
{
	fprintf(stderr, "Usage: %s <wktfile> <minx> <miny> <maxx> <maxy> <scalex> <scaley> <offsetx> <offsety> [<numlevels> <maxranges>] \n", me);
	exit(1);
}

void notice(const char *fmt, ...) {
    va_list ap;

    fprintf( stdout, "NOTICE: ");

    va_start (ap, fmt);
    vfprintf( stdout, fmt, ap);
    va_end(ap);
    fprintf( stdout, "\n" );
}

void log_and_exit(const char *fmt, ...) {
    va_list ap;

    fprintf( stdout, "ERROR: ");

    va_start (ap, fmt);
    vfprintf( stdout, fmt, ap);
    va_end(ap);
    fprintf( stdout, "\n" );
    exit(1);
}

string readWKT(const char* inputfile){
	
	ifstream ifs(inputfile);
    string content( (istreambuf_iterator<char>(ifs) ),
                    (istreambuf_iterator<char>()    ) );
    
    return content;
}

string scale(string wkt, double scaleX, double scaleY, double offsetX, double offsetY){
    ostringstream owkt;
    string numstr = "";
    numstr.clear();
    // counter of numbers
    int numcounter = 0;
    int snum;
    for(int i = 0; wkt[i] != '\0'; ++i) {
        if (isdigit(wkt[i]) || wkt[i] == '.') numstr += wkt[i];
        else{
            if (!numstr.empty()){
                if (numcounter % 2 == 1){
                    snum = static_cast<int>((stod(numstr) - offsetY) / scaleY);
                }else{
                    snum = static_cast<int>((stod(numstr) - offsetX) / scaleX);
                }
                numcounter++;
                owkt << snum;
                numstr.clear();
             }
             owkt << wkt[i];
        }
    }
    return owkt.str();
}

int disjoint(char* ptr){
	if ((ptr[0] == 'F') && (ptr[1] == 'F') && (ptr[3] == 'F') && (ptr[4] == 'F')) return 1;
	return 0;
}

int contains(char* ptr){
	if ((ptr[0] != 'F') && (ptr[6] == 'F') && (ptr[7] == 'F')) return 1;
	return 0;
}

int excludeinteriors(char* ptr) {
	if (ptr[0] == 'F') return 1;
	return 0;
}

GEOSGeometry* createBox(uint32_t minx, uint32_t miny, uint32_t maxx, uint32_t maxy){
	GEOSCoordSequence* cs = GEOSCoordSeq_create(5, 2);
	//quads[i] is minx miny maxx maxy
	GEOSCoordSeq_setX(cs, 0, minx);
    GEOSCoordSeq_setY(cs, 0, maxy);
    
    GEOSCoordSeq_setX(cs, 1, minx);
    GEOSCoordSeq_setY(cs, 1, miny);
    
    GEOSCoordSeq_setX(cs, 2, maxx);
    GEOSCoordSeq_setY(cs, 2, miny);
    
    GEOSCoordSeq_setX(cs, 3, maxx);
    GEOSCoordSeq_setY(cs, 3, maxy);
    
    GEOSCoordSeq_setX(cs, 4, minx);
    GEOSCoordSeq_setY(cs, 4, maxy);
    
    return GEOSGeom_createPolygon(GEOSGeom_createLinearRing(cs), NULL, 0);
}

int relation(GEOSGeometry* geom1, GEOSGeometry* geom2) {
/* Returns the relationship between two geometries. 
   0 if they are disjoint, 
   1 if geom2 is completely in geom1,
   2 if geom2 is partly in geom1
*/
	char *ptr;
	int r;
	ptr = GEOSRelate(geom1, geom2);
	if ( ! GEOSRelatePattern(geom1, geom2, ptr) )
	{
		GEOSGeom_destroy(geom1);
		GEOSGeom_destroy(geom2);
		free(ptr);
		log_and_exit("! RelatePattern");
	}
	//printf("Relate: %s\n", ptr); 
	
	
	if (disjoint(ptr)) r = 0;
    else if (contains(ptr)) r = 1; 
    else // there is some overlaps
    { 
        if (excludeinteriors(ptr)) r = 0; // overlap only in boundaries, we do not count it
        else r = 2; // some interior of geom2 is in geom1
    }          
	free(ptr);
	return r;
}

int getRangesInQuadTreeCell(vector<uint64_t>* ranges, vector<char>* relations, int maxdepth, int parentlevel, uint64_t parentcode, GEOSGeometry* qgeom, uint32_t minx, uint32_t miny, uint32_t maxx, uint32_t maxy){
	GEOSGeometry* geombox;
	int i, r;
	unsigned int j;
	long quadcode;
	// Recursive method that return morton ranges overlapping with the region for the specified domain
	uint32_t cx = minx + ((maxx - minx) >> 1);
	uint32_t cy = miny + ((maxy - miny) >> 1);
    int level = parentlevel + 1;
    
    uint32_t quads[NUMELEMQTC][4] =
	{
		{minx, miny, cx, cy}, // 0
		{minx, cy, cx, maxy}, // 1
		{cx, miny, maxx, cy}, // 2
		{cx, cy, maxx, maxy}  // 3
	};
    
   	int counter = 0;
   	
    for (i=0; i<NUMELEMQTC; i++) {
    	
        geombox = createBox(quads[i][0], quads[i][1], quads[i][2], quads[i][3]);
        
        r = relation(qgeom, geombox);
        
        char toadd = 0; // 0 is not to add, 1 is to add and is fully in, 2 is to add but not fully in
        
        if (r > 0)
        {
        	quadcode = (parentcode << 2) + i;
        	if ((r == 1) || (parentlevel == maxdepth)){
        		toadd = r;
        	}
        	else
        	{
        		vector<uint64_t>* sranges = new vector<uint64_t>();
        		vector<char>* srelations =  new vector<char>();
        		int scounter = getRangesInQuadTreeCell(sranges, srelations, maxdepth, level, quadcode, qgeom, quads[i][0], quads[i][1], quads[i][2], quads[i][3]);
        		if (scounter == NUMELEMQTC){
        			toadd = 2;
        		}else{
        			for (j=0; j<sranges->size(); j++) {
        				ranges->push_back((*sranges)[j]);
        			}
        			for (j=0; j<srelations->size(); j++) {
        				relations->push_back((*srelations)[j]);
        			}
        		}
        		free(sranges);
        		free(srelations);
        	}
        	
        	if (toadd > 0){
        		uint64_t diff = (numbits - level) << 1;
        		uint64_t minr = quadcode << diff;
        		uint64_t maxr = ((quadcode+1) << diff) - 1;
        		ranges->push_back(minr);
        		ranges->push_back(maxr);
        		counter++;
        		relations->push_back(toadd);
        	}
        }
    } 
    return counter;
}

int getRanges(vector<uint64_t>* ranges, vector<char>* relations, GEOSGeometry* qgeom){
	// Create first cell of Quad-Tree (the one in level 0 that covers all domain) 
	GEOSGeometry* dgeom = createBox(domain[0], domain[1], domain[2], domain[3]);

	// Set numlevels: automatic (if in init it was -1) or set in initialization
	int numberOfLevels;
	double darea;
	double qarea;
	if (numlevels < 0){
		double* parea = &darea;
		GEOSArea(dgeom, parea);
		parea = &qarea;
		GEOSArea(qgeom, parea);
		numberOfLevels = static_cast<int>(ceil(log2(darea/qarea)/2)) + NUMLEVELFACTOR;
		//printf("#levels: %d\n", nl); 
	}else numberOfLevels = numlevels; // We use the one set in initialization
	
	//cout << "C++ " << numberOfLevels << " " << darea << " " << qarea << endl;
	
	// Check if query geometry overlaps with whole Quad-Tree
	int r = relation(dgeom, qgeom);
	if (r > 0){ // There is overlap
		// Call to recursive method to fill ranges though-in the Quad-Tree
		getRangesInQuadTreeCell(ranges, relations, numberOfLevels, startlevel, startquadcode, qgeom, startquad[0], startquad[1], startquad[2], startquad[3]);
		return 1;
	}else return 0;
	
}

int mergeConsecutiveRanges(vector<uint64_t>* iranges, vector<uint64_t>* oranges){
	unsigned int numRanges = iranges->size() / 2;
	if (numRanges == 0) return 0;

	uint64_t mrangemin = (*iranges)[0];
	uint64_t mrangemax = (*iranges)[1];

	unsigned int i;
	uint64_t cmrangemin;
	uint64_t cmrangemax;
	for (i=1; i<numRanges; i++) {
		cmrangemin = (*iranges)[2*i];
		cmrangemax = (*iranges)[(2*i) + 1];
		if (mrangemax == cmrangemin - 1) mrangemax = cmrangemax;
		else{
			oranges->push_back(mrangemin);
			oranges->push_back(mrangemax);
			mrangemin = cmrangemin;
			mrangemax = cmrangemax;
		}
	}
	oranges->push_back(mrangemin);
	oranges->push_back(mrangemax);
	return 1;
}

int mergeRangesMaxRanges(vector<uint64_t>* iranges, vector<uint64_t>* oranges, int maxRanges){
    int i;
    
    int numRanges = iranges->size() / 2;
    
    if ((numRanges <= maxRanges) || (numRanges < 2)) return 0;

    int numRangesToMerge = numRanges - maxRanges;
    int numDiffs = numRanges - 1; 
    
    uint64_t* diffs = new uint64_t[numDiffs];
    uint64_t* cdiffs = new uint64_t[numDiffs];
    for (i = 0; i < numDiffs; i++){
        diffs[i] = (*iranges)[2*(i+1)] - (*iranges)[(2*i)+1];
    }
    memcpy(cdiffs, diffs, numDiffs * sizeof *diffs);
    
    sort(cdiffs, cdiffs + numDiffs);
    
    uint64_t tDiff = cdiffs[numRangesToMerge-1];
    
    int lowerDiffs = 0;
    for (i = 0; i < numDiffs; i++){
        if (diffs[i] < tDiff) lowerDiffs++;	
    }
    
    int equalToMerge = numRangesToMerge - lowerDiffs;
    
    int equalCounter = 0;
    i = 0;
	uint64_t mrangemin = (*iranges)[2*i];
	int initrange = 0;
    for (i = 0; i < numRanges; i++){
    	if (initrange == 1){
    		 mrangemin = (*iranges)[2*i];
    		 initrange = 0;
    	}
    	if (i < numDiffs){
    		if (diffs[i] > tDiff){
    			oranges->push_back(mrangemin);
    			oranges->push_back((*iranges)[(2*i)+1]);
    			initrange = 1;
    		}else if (diffs[i] == tDiff){
    			equalCounter++;
    			if (equalCounter > equalToMerge){
    				oranges->push_back(mrangemin);
    				oranges->push_back((*iranges)[(2*i)+1]);
    				initrange = 1;
    			}
    		}
    	}else{
    		 oranges->push_back(mrangemin);
    		 oranges->push_back((*iranges)[(2*i)+1]);
    	}
    }
    return 1;
}

void processWKT(vector<uint64_t>*& pranges, vector<char>*& prelations, string wkt, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels = -1, int mergeconsecutive = -1, int maximumranges = -1){
	// Set values to the global variabales
	numlevels = numberlevels;
	maxranges = maximumranges;
	numbits = MAXBITS; //Set initial to MAX, can be decreased later
	// Define the domain (scaled and offseted)
    domain[0] = static_cast<int>((minx - offsetx) / scalex);
    domain[1] = static_cast<int>((miny - offsety) / scaley);
    domain[2] = static_cast<int>((maxx - offsetx) / scalex);
    domain[3] = static_cast<int>((maxy - offsety) / scaley);
    if ((domain[0] < 0) || (domain[1] < 0) || (domain[2] < 0) || (domain[3] < 0)) log_and_exit("domain must contain only positive X and Y 32bit integers!");
    // Get the maximum value axises values (max of max x and max y)
    int dmaxxy;
    if (domain[2] > domain[3]) dmaxxy = domain[2]; else dmaxxy = domain[3];
    // Compute the number of bits required to fit the domain
	int fitsdomain = 1;
    while (fitsdomain == 1){
        if ((ONELONG << numbits) > dmaxxy) numbits--;
        else {
            fitsdomain = 0;
            numbits++;
        }
    }
    // Check number of bits is less that maximum (31)
    if ((numbits > MAXBITS)) log_and_exit("required #bits: %d. Maximum: %d", numbits, MAXBITS);
	// If numlevels is given (if provided is > 0) we check that there are not less levels than num bits
    if (numlevels > 0)
    {
        if (numlevels > numbits) log_and_exit("numlevels (%d) must be lower or equal to the required #bits (%d)", numlevels, numbits);
    }
  	// Define the smallest Quad-Tree domain that can fit all the given domain
  	// Quad-Tree domains must be powers of 2
    uint32_t minsq = 0;
    uint32_t maxsq = ONELONG << numbits; //excluded
    startlevel = 0;
    startquadcode = 0;
    startquad[0] = minsq;
    startquad[1] = minsq;
    startquad[2] = maxsq;
    startquad[3] = maxsq; 
    /*
    printf("domain: %d %d %d %d\n", domain[0], domain[1], domain[2], domain[3]);
    printf("#bits: %d\n", numbits); 
    printf("#levels: %d\n", numlevels);
    printf("startlevel: %d\n", startlevel);
    printf("startquadcode: %lu\n", startquadcode);
    printf("startquad: %u %u %u %u\n", startquad[0], startquad[1], startquad[2], startquad[3]);
	*/
    
	string pwkt;
	if (!double_equals(offsetx,0) || !double_equals(offsety,0) || !double_equals(scalex,1) || !double_equals(scaley,1)) pwkt = scale(wkt, scalex, scaley, offsetx, offsety);
	else pwkt = wkt;
	
	char wktca[MAXWKTLEN];
	strncpy(wktca, pwkt.c_str(), sizeof(wktca));
	wktca[sizeof(wktca) - 1] = 0;
	
	GEOSWKTReader* geomReader;
	geomReader = GEOSWKTReader_create();
	GEOSGeometry* qgeom = GEOSWKTReader_read(geomReader, wktca);
    
	vector<uint64_t>* ranges1 = new vector<uint64_t>();
	vector<char>* relations1 = new vector<char>();
	
	getRanges(ranges1, relations1, qgeom);
	
	GEOSGeom_destroy(qgeom);
	
	vector<uint64_t>* ranges2 = new vector<uint64_t>();
	vector<uint64_t>* ranges3 = new vector<uint64_t>();
	int status2 = 0;
	int status3 = 0;
	if ((mergeconsecutive > 0) || (maxranges > 0)){
		status2 = mergeConsecutiveRanges(ranges1, ranges2);
		if (maxranges > 0) status3 = mergeRangesMaxRanges(ranges2, ranges3, maxranges);
	}
	if (status3 == 1){
		pranges = ranges3;
		free(ranges1);
		free(ranges2);
	}
	else if (status2 == 1){
		pranges = ranges2;
		free(ranges1);
		free(ranges3);
	}
	else{
		pranges = ranges1;
		prelations = relations1;
		free(ranges2);
		free(ranges3);
	}
}

vector<uint64_t> getRangesWKT(string wkt, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels = -1, int mergeconsecutive = -1, int maximumranges = -1){
    initGEOS(notice, log_and_exit);
    vector<uint64_t>* pranges = NULL;
	vector<char>* prelations = NULL;
	processWKT(pranges, prelations, wkt, minx, miny, maxx, maxy, scalex, scaley, offsetx, offsety, numberlevels, mergeconsecutive, maximumranges);
    finishGEOS();
    return (*pranges);
}

vector<uint64_t> getRangesFile(char* filepath, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels = -1, int mergeconsecutive = -1, int maximumranges = -1){
    initGEOS(notice, log_and_exit);
    vector<uint64_t>* pranges = NULL;
	vector<char>* prelations = NULL;
	processWKT(pranges, prelations, readWKT(filepath), minx, miny, maxx, maxy, scalex, scaley, offsetx, offsety, numberlevels, mergeconsecutive, maximumranges);
    finishGEOS();
    return (*pranges);
}


int main(int argc, char **argv){
	// Set the global variables to the default values
	char* filepath;
    double minx, miny, maxx, maxy;
    double scalex, scaley, offsetx, offsety;
    int numberlevels = -1;
    int maximumranges = -1;
    int mergeconsecutive = -1;
    
    if ((argc < 10) || (argc > 13)) usage(argv[0]);
    filepath = argv[1];
    minx = atof(argv[2]);
    miny = atof(argv[3]);
    maxx = atof(argv[4]);
    maxy = atof(argv[5]);
    scalex = atof(argv[6]);
    scaley = atof(argv[7]);
    offsetx = atof(argv[8]);
    offsety = atof(argv[9]);
    if (argc > 10) numberlevels = atoi(argv[10]);
    if (argc > 11) maximumranges = atoi(argv[11]);
    if (argc > 12) mergeconsecutive = atoi(argv[12]);
    
    vector<uint64_t> ranges = getRangesFile(filepath, minx, miny, maxx, maxy, scalex, scaley, offsetx, offsety, numberlevels, mergeconsecutive, maximumranges);
    
    int numranges = ranges.size() / 2; 
    for (int j = 0; j < numranges; j++){
		printf("  %ld %ld\n", ranges[2*j], ranges[(2*j) + 1]);
	}
}
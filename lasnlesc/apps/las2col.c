#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>

#include "liblas.h"
#include "lascommon.h"

#include <limits.h>
#include <inttypes.h>
#include <endian.h>
#include <math.h>

#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#define boolean short
#define int64_t long long int

#define NUM_OF_ENTRIES      21
#define DEFAULT_NUM_READ_THREADS 16
#define DEFAULT_NUM_INPUT_FILES 2000
#define TOLERANCE 0.0000001
#define MAX_INT_31 2147483648.0

#define set_lock(lock, s) \
{ MT_lock_set(&lock, s);}
#define unset_lock(node, lock, s) \
{ MT_lock_unset(&lock, s);}




void print_header(FILE *file, LASHeaderH header, const char* file_name);

void usage()
{
    fprintf(stderr,"----------------------------------------------------------\n");
    fprintf(stderr,"    las2col (version %s) usage:\n", LAS_GetVersion());
    fprintf(stderr,"----------------------------------------------------------\n");
    fprintf(stderr,"\n");

    fprintf(stderr,"Convert a las file into columnar format (binary), outputs <input_file>_col_<entry_name>.dat (for all entries):\n");
    fprintf(stderr,"  las2col -i <input_file>.las\n");
    fprintf(stderr,"\n");

    fprintf(stderr,"Convert a las file into columnar format (binary), outputs <ouput_name>_col_<entry_name>.dat:\n");
    fprintf(stderr,"  las2col -i <input_file>.las <output_dir_path/prefix>\n");
    fprintf(stderr,"\n");

    fprintf(stderr,"Convert a las file into columnar format (binary), outputs <input_file>_col_<entry_name>.dat (only for xyziar entries) :\n");
    fprintf(stderr,"  las2col --parse xyziar -i <input_file>.las\n");
    fprintf(stderr,"\n");

    fprintf(stderr,"Convert a list of las files into columnar format (binary), outputs <input_file>_col_<entry_name>.dat (only for xyziar entries) :\n");
    fprintf(stderr,"  las2col -f <file_with_the_list_las_files> --parse xyziar <input_file>.las\n");
    fprintf(stderr,"\n");

    fprintf(stderr,"Convert a list of las files into columnar format (binary), outputs <input_file>_col_<entry_name>.dat (only for xyziar entries) :\n");
    fprintf(stderr,"  las2col -i <las_file_1> -i <las_file_2> --parse xyziar <input_file>.las\n");
    fprintf(stderr,"\n");

    fprintf(stderr,"Convert a list of las files into columnar format (binary) using <num_read_threads> threads (default is 16), outputs <input_file>_col_<entry_name>.dat (only for xyziar entries) :\n");
    fprintf(stderr,"  las2col --num_read_threads <number_of_threads> -i <las_file_1> -i <las_file_2> --parse xyziar <input_file>.las\n");
    fprintf(stderr,"\n");

    fprintf(stderr,"----------------------------------------------------------\n");
    fprintf(stderr," The '--parse txyz' flag specifies each entries of the LAS\n");
    fprintf(stderr," will be extracted. For example, 'txyzia'\n");
    fprintf(stderr," means that the first number of each line should be the\n");
    fprintf(stderr," gpstime, the next three numbers should be the x, y, and\n");
    fprintf(stderr," z coordinate, the next number should be the intensity\n");
    fprintf(stderr," and the next number should be the scan angle.\n");
    fprintf(stderr," The supported entries are:\n");
    fprintf(stderr,"   t - gpstime\n");
    fprintf(stderr,"   x - x coordinate as a double\n");
    fprintf(stderr,"   y - y coordinate as a double\n");
    fprintf(stderr,"   z - z coordinate as a double\n");
    fprintf(stderr,"   X - x coordinate as a decimal)\n");
    fprintf(stderr,"   Y - y coordinate as a decimal)\n");
    fprintf(stderr,"   Z - z coordinate as a decimal)\n");
    fprintf(stderr,"   a - scan angle\n");
    fprintf(stderr,"   i - intensity\n");
    fprintf(stderr,"   n - number of returns for given pulse\n");
    fprintf(stderr,"   r - number of this return\n");
    fprintf(stderr,"   c - classification number\n");
    fprintf(stderr,"   C - classification name\n");
    fprintf(stderr,"   u - user data (does not currently work)\n");
    fprintf(stderr,"   p - point source ID\n");
    fprintf(stderr,"   e - edge of flight line\n");
    fprintf(stderr,"   d - direction of scan flag\n");
    fprintf(stderr,"   R - red channel of RGB color\n");
    fprintf(stderr,"   G - green channel of RGB color\n");
    fprintf(stderr,"   B - blue channel of RGB color\n");
    fprintf(stderr,"   M - vertex index number\n\n");
    fprintf(stderr,"   k - Morton 2D code using X and Y (no scale and no offset)\n\n");

    fprintf(stderr," The '--moffset 8600000,40000000' flag specifies an global offset in X and Y \n");
    fprintf(stderr," to be used when computing the Morton 2D code. Values must be unscaled \n");

    fprintf(stderr," The '--check 0.01,0.01' flag checks suitability to compute Morton 2D codes \n");
    fprintf(stderr," It checks specified scale matches the one in input file. \n");
    fprintf(stderr," If moffset is provided it also checks that obtained Morton 2D codes \n");
    fprintf(stderr," will be consistent, i.e. global X,Y within [0,2^31] \n\n");

    fprintf(stderr, "\nFor more information, see the full documentation for las2col at:\n"
            " http://liblas.org/browser/trunk/doc/las2col.txt\n");
    fprintf(stderr,"----------------------------------------------------------\n");
}

/*Global structures*/
#define MT_Lock pthread_mutex_t
#define MT_set_lock(p) pthread_mutex_lock(p)
#define MT_unset_lock(p) pthread_mutex_unlock(p)
#define MT_lock_init(p) pthread_mutex_init(p,NULL)
#define MT_lock_destroy(p) pthread_mutex_destroy(p)

#define MT_Cond pthread_cond_t
#define MT_cond_wait(p,t) pthread_cond_wait(p,t)
#define MT_cond_init(p) pthread_cond_init(p,NULL)
#define MT_cond_destroy(p) pthread_cond_destroy(p)

typedef void (*f_ptr)( void );

MT_Lock dataLock;
MT_Cond mainCond, writeTCond, readCond;
int entries[NUM_OF_ENTRIES];
double (*entriesFunc[NUM_OF_ENTRIES])();
int entriesType[NUM_OF_ENTRIES];
char **files_name_in = NULL;
int files_in_index = 0 ;
int skip_invalid = FALSE;
int verbose = TRUE;
struct writeT **data = NULL;
struct writeT *dataWriteT = NULL;
int stop;

typedef enum {
    ENTRY_x,
    ENTRY_y,
    ENTRY_z,
    ENTRY_X,
    ENTRY_Y,
    ENTRY_Z,
    ENTRY_t,
    ENTRY_i,
    ENTRY_a,
    ENTRY_r,
    ENTRY_c,
    ENTRY_u,
    ENTRY_n,
    ENTRY_R,
    ENTRY_G,
    ENTRY_B,
    ENTRY_M,
    ENTRY_p,
    ENTRY_e,
    ENTRY_d,
    ENTRY_k
} ENTRIES;

struct writeThreadArgs {
    int id;
    FILE *out;
};

struct writeT {
    long num_points;
    char* values;
    int type;
};

struct readThreadArgs {
    int id;
    int num_read_threads;
    int num_of_entries;
    int check;
    int64_t global_offset_x;
    int64_t global_offset_y;
    double scale_x;
    double scale_y;
};

void* writeFile(void *arg) {
    struct writeThreadArgs *wTA = (struct writeThreadArgs*) arg;

    /*Obtain lock over data to get the pointer*/
    while (stop == 0) {
        MT_set_lock(&dataLock);
        while ((stop == 0) && (dataWriteT == NULL || (dataWriteT && dataWriteT[wTA->id].values == NULL))) {
            /*Sleep and wait for data to be read*/
            MT_cond_wait(&writeTCond,&dataLock);
        }
        //Release the lock
        MT_unset_lock(&dataLock);

        if (stop) {
            return NULL;
        }

        fwrite(dataWriteT[wTA->id].values, dataWriteT[wTA->id].type, dataWriteT[wTA->id].num_points, wTA->out);
        MT_set_lock(&dataLock);
        free(dataWriteT[wTA->id].values);
        dataWriteT[wTA->id].values = NULL;
        MT_unset_lock(&dataLock);
        fflush(wTA->out);

        /*Wake up the main*/
        pthread_cond_broadcast(&mainCond);
    }
    return NULL;
}

void* readFile(void *arg) {
    struct readThreadArgs *rTA = (struct readThreadArgs*) arg;
    LASReaderH reader = NULL;
    LASHeaderH header = NULL;
    LASPointH p = NULL;
    unsigned int index = 0;
    int read_index = 0;
    char *file_name_in = NULL;
    int i, j;

    while(1) {
        file_name_in = NULL;
        /*Get next file to read*/
        MT_set_lock(&dataLock);
        file_name_in = files_name_in[files_in_index];
        if (file_name_in == NULL) {
            MT_unset_lock(&dataLock);
            return NULL;
        }
        read_index = (files_in_index % rTA->num_read_threads);
        files_in_index++;

        struct writeT *dataWriteTT = (struct writeT*) malloc(sizeof(struct writeT)*rTA->num_of_entries);
        /*Lets read the data*/
        reader = LASReader_Create(file_name_in);
        if (!reader) {
            LASError_Print("Unable to read file");
            MT_unset_lock(&dataLock);
            exit(1);
        }
        MT_unset_lock(&dataLock);

        header = LASReader_GetHeader(reader);
        if (!header) {
            LASError_Print("Unable to fetch header for file");
            exit(1);
        }

        if (verbose)
        {
            print_header(stderr, header, file_name_in);
        }

        /*Allocate arrays for the columns*/
	long num_points = LASHeader_GetPointRecordsCount(header);
	for (i = 0; i < rTA->num_of_entries; i++) {
		dataWriteTT[i].num_points = num_points;
		dataWriteTT[i].values = malloc(entriesType[i]*num_points);
		dataWriteTT[i].type = entriesType[i];
	}

	/*Changes for Oscar's new Morton code function*/
	//unsigned int factorX = (unsigned int) (LASHeader_GetOffsetX(header) / LASHeader_GetScaleX(header));
	//unsigned int factorY = (unsigned int) (LASHeader_GetOffsetY(header) / LASHeader_GetScaleY(header));

    /*Compute factors to add to X and Y and cehck sanity of generated codes*/
    double file_scale_x = LASHeader_GetScaleX(header);
    double file_scale_y = LASHeader_GetScaleY(header);
    double file_scale_z = LASHeader_GetScaleZ(header);
    //printf("The scales are x:%lf y:%lf z:%lf\n", file_scale_x, file_scale_y, file_scale_z);

	/* scaled offsets to add for the morton encoding */
	int64_t factorX =  ((int64_t) (LASHeader_GetOffsetX(header) / file_scale_x)) - rTA->global_offset_x;
	int64_t factorY =  ((int64_t) (LASHeader_GetOffsetY(header) / file_scale_y)) - rTA->global_offset_y;

	if (rTA->check)
	{
	        // Check specified scales are like in the LAS file
		if (fabs(rTA->scale_x - file_scale_x) > TOLERANCE){
			fprintf(stderr, "ERROR: x scale in input file (%lf) does not match specified x scale (%lf)\n",file_scale_x, rTA->scale_x);
			exit(1);
		}
		if (fabs(rTA->scale_y - file_scale_y) > TOLERANCE){
			fprintf(stderr, "ERROR: y scale in input file (%lf) does not match specified y scale (%lf)\n",file_scale_y, rTA->scale_y);
			exit(1);
		}
		/* Check that the extent of the file (taking into account the global offset)
		 * is within 0,2^31 */
		double check_min_x = 1.0 + LASHeader_GetMinX(header) - (((double) rTA->global_offset_x) * rTA->scale_x);
		if (check_min_x < TOLERANCE) {
			fprintf(stderr, "ERROR: Specied X global offset is too large. (MinX - (GlobalX*ScaleX)) < 0\n");
			exit(1);
		}
		double check_min_y = 1.0 + LASHeader_GetMinY(header) - (((double) rTA->global_offset_y) * rTA->scale_y);
		if (check_min_y < TOLERANCE) {
			fprintf(stderr, "ERROR: Specied Y global offset is too large. (MinY - (GlobalY*ScaleY)) < 0\n");
			exit(1);
		}
		double check_max_x = LASHeader_GetMaxX(header) - (((double) rTA->global_offset_x) * rTA->scale_x);
		if (check_max_x > (MAX_INT_31 * rTA->scale_x)) {
			fprintf(stderr, "ERROR: Specied X global offset is too small. (MaxX - (GlobalX*ScaleX)) > (2^31)*ScaleX\n");
			exit(1);
		}
		double check_max_y = LASHeader_GetMaxY(header) - (((double) rTA->global_offset_y) * rTA->scale_y);
		if (check_max_y > (MAX_INT_31 * rTA->scale_y)) {
			fprintf(stderr, "ERROR: Specied Y global offset is too small. (MaxY - (GlobalY*ScaleY)) > (2^31)*ScaleY\n");
			exit(1);
		}
	}

        p = LASReader_GetNextPoint(reader);
        index = 0;
        while (p)
        {
            if (skip_invalid && !LASPoint_IsValid(p)) {
                if (verbose) {
                    LASError_Print("Skipping writing invalid point...");
                }
                p = LASReader_GetNextPoint(reader);
                index -=1;
                continue;
            }

            LASColorH color = NULL;
            for (j = 0; j < rTA->num_of_entries; j++) {
                uint64_t res;
                switch (entries[j]) {
                    case ENTRY_x:
                    case ENTRY_y:
                    case ENTRY_z:
                    case ENTRY_t:
                        ((double*) dataWriteTT[j].values)[index] = entriesFunc[j](p);
                        //printf(" Point is:%lf\n", ((double*) dataWriteTT[j].values)[index]);
                        break;
                    case ENTRY_X:
                        ((int*) dataWriteTT[j].values)[index] = entriesFunc[j](p) / file_scale_x;
                        break;
                    case ENTRY_Y:
                        ((int*) dataWriteTT[j].values)[index] = entriesFunc[j](p) / file_scale_y;
                        break;
                    case ENTRY_Z:
                        ((int*) dataWriteTT[j].values)[index] = entriesFunc[j](p) / file_scale_z;
                        break;
                    case ENTRY_i:
                    case ENTRY_a:
                    case ENTRY_r:
                    case ENTRY_c:
                    case ENTRY_u:
                    case ENTRY_n:
                    case ENTRY_p:
                    case ENTRY_e:
                    case ENTRY_d:
                        ((int*) dataWriteTT[j].values)[index] = entriesFunc[j](p);
                        break;
                    case ENTRY_k:
                        entriesFunc[j](&res, p, factorX, factorY);
                        ((int64_t*)dataWriteTT[j].values)[index] = res;
                        break;
                    case ENTRY_R:
                    case ENTRY_G:
                    case ENTRY_B:
                        color = (color == NULL) ? LASPoint_GetColor(p) : color;
                        dataWriteTT[j].values[index] = (double) entriesFunc[j](color);
                        break;
                    case ENTRY_M:
                        dataWriteTT[j].values[index] = index;
                        break;
                    default:
                        LASError_Print("las2col:readFile: Invalid Entry.");
                }
            }
            if (color != NULL)
                LASColor_Destroy(color);

            p = LASReader_GetNextPoint(reader);
            index +=1;
        }
        if (verbose)
            printf("Num of points:%d %ld for file:%s \n", index, num_points, file_name_in);

        /*Give the data to the writer threads*/
        MT_set_lock(&dataLock);
        LASHeader_Destroy(header);
        header = NULL;
        LASReader_Destroy(reader);
	    reader = NULL;

        /*TODO: make sure you are not overtaking other reading threads*/
        while (data[read_index] != NULL) {
            MT_cond_wait(&readCond, &dataLock);
        }
        data[read_index] = dataWriteTT;
        /*Wake up the main*/
        pthread_cond_broadcast(&mainCond);
        MT_unset_lock(&dataLock);

    }
    return NULL;
}

int doesFileExist(const char *filename) {
    struct stat st;
    int result = stat(filename, &st);
    return result == 0;
}

int64_t EncodeMorton2D_1(unsigned int rawx, unsigned int rawy){
    int64_t answer = 0;
    int64_t i;
    for (i = 0; i < (sizeof(int64_t)* CHAR_BIT)/2; ++i) {
        answer |= ((rawy & ((int64_t)1 << i)) << i) | ((rawx & ((int64_t)1 << i)) << (i + 1));
    }
    return answer;
}

uint64_t Expand1(uint32_t a)
{
	uint64_t b = a & 0x7fffffff;               // b = ---- ---- ---- ---- ---- ---- ---- ---- 0edc ba98 7654 3210 fedc ba98 7654 3210
	b = (b ^ (b <<  16)) & 0x0000ffff0000ffff; // b = ---- ---- ---- ---- 0edc ba98 7654 3210 ---- ---- ---- ---- fedc ba98 7654 3210
	b = (b ^ (b <<  8))  & 0x00ff00ff00ff00ff; // b = ---- ---- 0edc ba98 ---- ---- 7654 3210 ---- ---- fedc ba98 ---- ---- 7654 3210
	b = (b ^ (b <<  4))  & 0x0f0f0f0f0f0f0f0f; // b = ---- 0edc ---- ba98 ---- 7654 ---- 3210 ---- fedc ---- ba98 ---- 7654 ---- 3210
	b = (b ^ (b <<  2))  & 0x3333333333333333; // b = --0e --dc --ba --98 --76 --54 --32 --10 --fe --dc --ba --98 --76 --54 --32 --10
	b = (b ^ (b <<  1))  & 0x5555555555555555; // b = -0-e -d-c -b-a -9-8 -7-6 -5-4 -3-2 -1-0 -f-e -d-c -b-a -9-8 -7-6 -5-4 -3-2 -1-0
	return b;
}

int64_t morton2D_encode(int64_t *answer, LASPointH p, unsigned int factorX, unsigned int factorY){
    unsigned int rawx = ((unsigned int) LASPoint_GetRawX(p)) + factorX;
    unsigned int rawy = ((unsigned int) LASPoint_GetRawY(p)) + factorY;
    *answer = EncodeMorton2D_1(rawx, rawy);
    return *answer;
}

/*Changes for Oscar's new Morton code function*/
uint64_t morton2D_encodeOscar(uint64_t *answer, LASPointH p, unsigned int factorX, unsigned int factorY){
	uint32_t x = (uint32_t) (((int64_t) LASPoint_GetRawX(p)) + factorX);
	uint32_t y = (uint32_t) (((int64_t) LASPoint_GetRawY(p)) + factorY);
	*answer = (Expand1(x) << 1) + Expand1(y);

    return *answer;
}

int64_t S64(const char *s) {
	int64_t i;
	char c ;
	int scanned = sscanf(s, "%" SCNd64 "%c", &i, &c);
	if (scanned == 1) return i;
	fprintf(stderr, "ERROR: parsing string to int64_t.\n");
	exit(1);
}

int main(int argc, char *argv[])
{
    /*Initialize the catalog locks*/
    MT_lock_init(&dataLock);
    MT_cond_init(&mainCond);
    MT_cond_init(&writeTCond);
    MT_cond_init(&readCond);

    char* file_name_in = 0;
    char* file_name_out = 0;
    char separator_sign = ' ';
    char* parse_string = "xyz";
    char* buffer;
    char printstring[256];
    LASReaderH reader = NULL;
    LASHeaderH header = NULL;
    LASPointH p = NULL;
    FILE** files_out = NULL;
    int len, j;
    int64_t mortonkey = 0;
    unsigned int index = 0;
    int num_files_in = 0, num_files_out = 0, num_files, num_of_entries=0, check = 0, num_read_threads = DEFAULT_NUM_READ_THREADS;
    int i;
    pthread_t *writeThreads = NULL;
    pthread_t *readThreads = NULL;
    struct readThreadArgs *dataRead = NULL;
    boolean input_file = FALSE;
    int64_t global_offset_x = 0;
    int64_t global_offset_y = 0;
    double scale_x;
    double scale_y;


    if (argc == 1) {
        usage();
        exit(0);
    }

    /*Allocate space for input files*/
    files_name_in = (char**) malloc(sizeof(char*)*DEFAULT_NUM_INPUT_FILES);

    for (i = 1; i < argc; i++)
    {
        if (    strcmp(argv[i],"-h") == 0 ||
                strcmp(argv[i],"--help") == 0
           )
        {
            usage();
            exit(0);
        }
        else if (   strcmp(argv[i],"-v") == 0 ||
                strcmp(argv[i],"--verbose") == 0
                )
        {
            verbose = TRUE;
        }
        else if ( strcmp(argv[i],"--num_read_threads") == 0)
        {
            num_read_threads = atoi(argv[++i]);
        }
        else if (   strcmp(argv[i],"-s") == 0 ||
                strcmp(argv[i],"--skip_invalid") == 0
                )
        {
            skip_invalid = TRUE;
        }
        else if (   strcmp(argv[i], "--parse") == 0 ||
                strcmp(argv[i], "-parse") == 0
                )
        {
            i++;
            parse_string = argv[i];
        }
        else if (   strcmp(argv[i], "--moffset") == 0 ||
                strcmp(argv[i], "-moffset") == 0
                )
        {
            i++;
            buffer = strtok (argv[i], ",");
            j = 0;
            while (buffer) {
                if (j == 0) {
                    global_offset_x = S64(buffer);
                }
                else if (j == 1) {
                    global_offset_y = S64(buffer);
                }
                j++;
                buffer = strtok (NULL, ",");
                while (buffer && *buffer == '\040')
                    buffer++;
            }
            if (j != 2){
                fprintf(stderr, "Only two int64_t are required in moffset option!\n");
                exit(1);
            }

        }
        else if (   strcmp(argv[i], "--check") == 0 ||
                strcmp(argv[i], "-check") == 0
                )
        {
            i++;
            check = 1;
            buffer = strtok (argv[i], ",");
            j = 0;
            while (buffer) {
                if (j == 0) {
                    sscanf(buffer, "%lf", &scale_x);
                }
                else if (j == 1) {
                    sscanf(buffer, "%lf", &scale_y);
                }
                j++;
                buffer = strtok (NULL, ",");
                while (buffer && *buffer == '\040')
                    buffer++;
            }
            if (j != 2){
                fprintf(stderr, "Only two doubles are required in moffset option!\n");
                exit(1);
            }
        }
        else if (   strcmp(argv[i],"--input") == 0  ||
                strcmp(argv[i],"-input") == 0   ||
                strcmp(argv[i],"-i") == 0       ||
                strcmp(argv[i],"-in") == 0
                )
        {
            i++;
            files_name_in[num_files_in++] = argv[i];

            if (num_files_in % DEFAULT_NUM_INPUT_FILES)
                files_name_in = (char**) realloc(files_name_in, (num_files_in*2)*sizeof(char*));
        }
        else if (strcmp(argv[i],"--file") == 0  ||
                strcmp(argv[i],"-file") == 0   ||
                strcmp(argv[i],"-f") == 0
                )
        {
            i++;
            int read;
            char line_buffer[BUFSIZ];
            FILE* in = NULL;

            in = fopen(argv[i], "r");
            if (!in) {
                fprintf(stderr, "ERROR: the path for file containing the input files is invalid %s\n", argv[i]);
                exit(1);
            }
            while (fgets(line_buffer, sizeof(line_buffer), in)) {
                line_buffer[strlen(line_buffer)-1]='\0';
                files_name_in[num_files_in++] = strdup(line_buffer);

                if (num_files_in % DEFAULT_NUM_INPUT_FILES)
                    files_name_in = (char**) realloc(files_name_in, (num_files_in*2)*sizeof(char*));
            }
            fclose(in);
            input_file = TRUE;
        }
        else if ((num_files_in != 0) && num_files_out == 0)
        {
            file_name_out = argv[i];
            num_files_out++;
        }
        else
        {
            fprintf(stderr, "ERROR: unknown argument '%s'\n",argv[i]);
            usage();
            exit(1);
        }
    } /* end looping through argc/argv */
    num_of_entries = strlen(parse_string);

    if (num_files_in == 0)
    {
        LASError_Print("No input filename was specified");
        usage();
        exit(1);
    }
    files_name_in[num_files_in] = NULL;
    num_files = num_files_in;

    /*Entries metadata*/
    i = 0;
    for (;;)
    {
        switch (parse_string[i])
        {
                /* // the morton code on xy */
            case 'k':
                entries[i] = ENTRY_k;
                entriesType[i] = sizeof(int64_t);
	            /*Changes for Oscar's new Morton code function*/
                //entriesFunc[i] = (void*)morton2D_encode;
                entriesFunc[i] = (void*)morton2D_encodeOscar;
                break;
                /* // the x coordinate  double*/
            case 'x':
                entries[i] = ENTRY_x;
                entriesType[i] = sizeof(double);
                entriesFunc[i] = (void*)LASPoint_GetX;
                break;
                /* // the y coordinate double*/
            case 'y':
                entries[i] = ENTRY_y;
                entriesType[i] = sizeof(double);
                entriesFunc[i] = (void*)LASPoint_GetY;
                break;
                /* // the z coordinate double*/
            case 'z':
                entries[i] = ENTRY_z;
                entriesType[i] = sizeof(double);
                entriesFunc[i] = (void*)LASPoint_GetZ;
                break;
                /* // the X coordinate decimal*/
            case 'X':
                entries[i] = ENTRY_X;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetX;
                break;
                /* // the y coordinate decimal*/
            case 'Y':
                entries[i] = ENTRY_Y;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetY;
                break;
                /* // the z coordinate decimal*/
            case 'Z':
                entries[i] = ENTRY_Z;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetZ;
                break;
                /* // the gps-time */
            case 't':
                entries[i] = ENTRY_t;
                entriesType[i] = sizeof(double);
                entriesFunc[i] = (void*)LASPoint_GetTime;
                break;
                /* // the intensity */
            case 'i':
                entries[i] = ENTRY_i;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetIntensity;
                break;
                /* the scan angle */
            case 'a':
                entries[i] = ENTRY_a;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetScanAngleRank;
                break;
                /* the number of the return */
            case 'r':
                entries[i] = ENTRY_r;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetReturnNumber;
                break;
                /* the classification */
            case 'c':
                entries[i] = ENTRY_c;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetClassification;
                break;
                /* the user data */
            case 'u':
                entries[i] = ENTRY_u;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetUserData;
                break;
                /* the number of returns of given pulse */
            case 'n':
                entries[i] = ENTRY_n;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetNumberOfReturns;
                break;
                /* the red channel color */
            case 'R':
                entries[i] = ENTRY_R;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASColor_GetRed;
                break;
                /* the green channel color */
            case 'G':
                entries[i] = ENTRY_G;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASColor_GetGreen;
                break;
                /* the blue channel color */
            case 'B':
                entries[i] = ENTRY_B;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASColor_GetBlue;
                break;
            case 'M':
                entries[i] = ENTRY_M;
                entriesType[i] = sizeof(unsigned int);
                break;
            case 'p':
                entries[i] = ENTRY_p;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetPointSourceId;
                break;
                /* the edge of flight line flag */
            case 'e':
                entries[i] = ENTRY_e;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetFlightLineEdge;
                break;
                /* the direction of scan flag */
            case 'd':
                entries[i] = ENTRY_d;
                entriesType[i] = sizeof(int);
                entriesFunc[i] = (void*)LASPoint_GetScanDirection;
                break;
        }
        i++;
        if (parse_string[i] == 0)
        {
            break;
        }
    }

    /*Prepare the output files*/
    if (file_name_out == NULL)
    {
        len = (int)strlen(file_name_in);
        file_name_out = LASCopyString(file_name_in);
        if (file_name_out[len-3] == '.' && file_name_out[len-2] == 'g' && file_name_out[len-1] == 'z')
        {
            len = len - 4;
        }
        while (len > 0 && file_name_out[len] != '.')
        {
            len--;
        }
        file_name_out[len] = '\0';
    }
    char *str = malloc(sizeof(char)*(strlen(file_name_out)+12));
    files_out = (FILE**) malloc(sizeof(FILE*)*num_of_entries);
    for (i = 0; i < num_of_entries; i++) {
        sprintf(str, "%s_col_%c.dat", file_name_out, parse_string[i]);
        if(doesFileExist(str)) {
            remove(str);
        }

        files_out[i] = fopen(str, "wb");

        if (files_out[i] == 0) {
            LASError_Print("Could not open file for write");
            usage();
            exit(1);
        }
    }
    free(str);

    /*Initialize structures for the reading threads*/
    //data = (struct writeT**) malloc(num_read_threads*sizeof(struct writeT*)); //Malloc is more efficient than calloc
    data = (struct writeT**) calloc(num_read_threads, sizeof(struct writeT*));

    dataRead = (struct readThreadArgs*) malloc(sizeof(struct readThreadArgs)*num_read_threads);
    /* Launch read Threads */
    stop = 0;
    readThreads = (pthread_t*) malloc(sizeof(pthread_t)*num_read_threads);
    for (i=0; i < num_read_threads; i++) {
        dataRead[i].id = i;
        dataRead[i].num_read_threads = num_read_threads;
        dataRead[i].num_of_entries = num_of_entries;
        dataRead[i].check = check;
        dataRead[i].global_offset_x = global_offset_x;
        dataRead[i].global_offset_y = global_offset_y;
        dataRead[i].scale_x = scale_x;
        dataRead[i].scale_y = scale_y;
        pthread_create(&readThreads[i], NULL, readFile, (void*)dataRead);
    }

    int writeIndex = 0;
    writeThreads = (pthread_t*) malloc(sizeof(pthread_t)*num_of_entries);

    /* Launch Threads */
    struct writeThreadArgs *dataWrite = (struct writeThreadArgs *) malloc(sizeof(struct writeThreadArgs) *num_of_entries);
    for (i = 0; i < num_of_entries; i++) {
        dataWrite[i].id = i;
        dataWrite[i].out = files_out[i];
        pthread_create(&writeThreads[i], NULL, writeFile, (void*)(&dataWrite[i]));
    }
    sleep(1);
    //Do we need to comment this one out!?
    int done = 0;
    while (num_files) {
        /*Obtain lock over data to get the pointer*/
        MT_set_lock(&dataLock);
        dataWriteT = data[writeIndex];
        while (dataWriteT == NULL) {
            /*Sleep and wait for data to be read*/
            MT_cond_wait(&mainCond,&dataLock);
            dataWriteT = data[writeIndex];
        }
        data[writeIndex] = NULL;
        //Release the lock

        /*Tell the write threads there is new data*/
        pthread_cond_broadcast(&writeTCond);

        /*Tell the read threads there is a new buf empty*/
        pthread_cond_broadcast(&readCond);
        MT_unset_lock(&dataLock);

        /*Keep looping*/
        writeIndex++;
        writeIndex = (writeIndex % num_read_threads);

        MT_set_lock(&dataLock);
        while (done == 0) {
            /*Sleep and wait for data to be read*/
            MT_cond_wait(&mainCond,&dataLock);
            done = 1;
            for (i = 0; i < num_of_entries; i++) {
                if (dataWriteT[i].values != NULL) {
                    done = 0;
                    break;
                }
            }
        }
        num_files--;
        if (verbose)
            printf("Files to go %d\n", num_files);
        free(dataWriteT);
        dataWriteT = NULL;
        done = 0;
        MT_unset_lock(&dataLock);
    }

    /*Tell the write threads to exit*/
    MT_set_lock(&dataLock);
    stop = 1;
    pthread_cond_broadcast(&writeTCond);
    MT_unset_lock(&dataLock);

    /* Wait for Threads to Finish */
    for (i=0; i<num_of_entries; i++) {
        pthread_join(writeThreads[i], NULL);
    }
    free(dataWrite);
    free(writeThreads);

    MT_cond_destroy(&readCond);
    MT_cond_destroy(&writeTCond);
    MT_cond_destroy(&mainCond);
    MT_lock_destroy(&dataLock);

    for (i = 0; i < num_of_entries; i++) {
        fflush(files_out[i]);
        if (verbose)
            printf("close file %d\n", i);
        fsync(files_out[i]);
        fclose(files_out[i]);
    }
    free(files_out);
    if (input_file) {
        for (i=0 ; i < num_files_in; i++)
            free(files_name_in[i]);

        free(files_name_in);
    }

    free(dataRead);

    if (readThreads)
        free(readThreads);

    return 0;
}

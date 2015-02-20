#include<stdlib.h>
#include<stdio.h>
#include<string.h>

#define TUPLE_SIZE 6

void usage() {
    fprintf(stderr, "vox2txt <input_filename> <output_filename>\n");
}
int main(int argc, char** argv) {
    FILE *file_in = NULL, *file_out = NULL;
    int num_tuples = 0, read, written, size = TUPLE_SIZE;
    char buffer[TUPLE_SIZE];
    
    if (argc != 3) {
        usage();
        exit(1);
    }

    file_in = fopen(argv[1],"r");
    if (!file_in) {
        fprintf(stderr, "ERROR: it was not possible to open input file %s\n", argv[0]);
        exit(1);
    }

    file_out = fopen(argv[2],"w");
    if (!file_out) {
        fprintf(stderr, "ERROR: it was not possible to open output file %s\n", argv[1]);
        exit(1);
    }

    while (feof(file_in) == 0) {
       read = fread(buffer, 1, size, file_in);
       short *plane = (short *) &buffer[0];
       short *col = (short*) &buffer[2];
       short *row = (short*) &buffer[4];
       written = fprintf(file_out,"%u, %u, %u\n", *plane, *col, *row);
       num_tuples++;
    }
    fprintf(stderr, "%d tuples where converted from voxel format to CSV format\n", num_tuples);

    fflush(file_in);
    fclose(file_in);
    fclose(file_out);
    return 0;
}


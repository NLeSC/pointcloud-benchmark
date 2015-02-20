// This C class contains method that can be used to decode the morton keys generate by the binary loaders

int64_t Compact(int64_t m)
{
    m &= 0x5555555555555555;                  // m = -f-e -d-c -b-a -9-8 -7-6 -5-4 -3-2 -1-0 -f-e -d-c -b-a -9-8 -7-6 -5-4 -3-2 -1-0
    m = (m ^ (m >>  1)) & 0x3333333333333333; // m = --fe --dc --ba --98 --76 --54 --32 --10 --fe --dc --ba --98 --76 --54 --32 --10
    m = (m ^ (m >>  2)) & 0x0f0f0f0f0f0f0f0f; // m = ---- fedc ---- ba98 ---- 7654 ---- 3210 ---- fedc ---- ba98 ---- 7654 ---- 3210
    m = (m ^ (m >>  4)) & 0x00ff00ff00ff00ff; // m = ---- ---- fedc ba98 ---- ---- 7654 3210 ---- ---- fedc ba98 ---- ---- 7654 3210
    m = (m ^ (m >>  8)) & 0x0000ffff0000ffff; // m = ---- ---- ---- ---- fedc ba98 7654 3210 ---- ---- ---- ---- fedc ba98 7654 3210
    m = (m ^ (m >>  16)) & 0x00000000ffffffff; //m = ---- ---- ---- ---- ---- ---- ---- ---- fedc ba98 7654 3210 fedc ba98 7654 3210

    return (int32_t) m;
}

int32_t DecodeMorton2DX(int64_t mortonCode)
{
    return Compact(mortonCode >> 1);
}

int32_t DecodeMorton2DY(int64_t mortonCode)
{
    return Compact(mortonCode);
}

str
mortonGetX(double *out, int64_t *mortonCode, double *scaleX, int64_t *globalOffset)
{
    int32_t unscalled_code = DecodeMorton2DX(*mortonCode);
    *out = (unscalled_code + (*globalOffset)) * (*scaleX);
    return MAL_SUCCEED;
}

str
mortonGetY(double *out, int64_t *mortonCode, double *scaleY, int64_t *globalOffset)
{
    int32_t unscalled_code = DecodeMorton2DY(*mortonCode);
    *out = (unscalled_code + (*globalOffset)) * (*scaleY);
    return MAL_SUCCEED;
}

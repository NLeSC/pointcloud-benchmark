#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################

def Expand(v):
    b = v & 0x7fffffff                         # b = ---- ---- ---- ---- ---- ---- ---- ---- 0edc ba98 7654 3210 fedc ba98 7654 3210
    b = (b ^ (b <<  16)) & 0x0000ffff0000ffff  # b = ---- ---- ---- ---- 0edc ba98 7654 3210 ---- ---- ---- ---- fedc ba98 7654 3210
    b = (b ^ (b <<  8))  & 0x00ff00ff00ff00ff  # b = ---- ---- 0edc ba98 ---- ---- 7654 3210 ---- ---- fedc ba98 ---- ---- 7654 3210
    b = (b ^ (b <<  4))  & 0x0f0f0f0f0f0f0f0f  # b = ---- 0edc ---- ba98 ---- 7654 ---- 3210 ---- fedc ---- ba98 ---- 7654 ---- 3210
    b = (b ^ (b <<  2))  & 0x3333333333333333  # b = --0e --dc --ba --98 --76 --54 --32 --10 --fe --dc --ba --98 --76 --54 --32 --10
    b = (b ^ (b <<  1))  & 0x5555555555555555  # b = -0-e -d-c -b-a -9-8 -7-6 -5-4 -3-2 -1-0 -f-e -d-c -b-a -9-8 -7-6 -5-4 -3-2 -1-0
    return b

def EncodeMorton2D(x, y):
    return (Expand(x) << 1) + Expand(y)

def Compact(m):
    m &= 0x5555555555555555                   # m = -f-e -d-c -b-a -9-8 -7-6 -5-4 -3-2 -1-0 -f-e -d-c -b-a -9-8 -7-6 -5-4 -3-2 -1-0
    m = (m ^ (m >> 1))  & 0x3333333333333333  # m = --fe --dc --ba --98 --76 --54 --32 --10 --fe --dc --ba --98 --76 --54 --32 --10
    m = (m ^ (m >> 2))  & 0x0f0f0f0f0f0f0f0f  # m = ---- fedc ---- ba98 ---- 7654 ---- 3210 ---- fedc ---- ba98 ---- 7654 ---- 3210
    m = (m ^ (m >> 4))  & 0x00ff00ff00ff00ff  # m = ---- ---- fedc ba98 ---- ---- 7654 3210 ---- ---- fedc ba98 ---- ---- 7654 3210
    m = (m ^ (m >> 8))  & 0x0000ffff0000ffff  # m = ---- ---- ---- ---- fedc ba98 7654 3210 ---- ---- ---- ---- fedc ba98 7654 3210
    m = (m ^ (m >> 16)) & 0x00000000ffffffff  # m = ---- ---- ---- ---- ---- ---- ---- ---- fedc ba98 7654 3210 fedc ba98 7654 3210
    return m

def DecodeMorton2DX(mortonCode):
    return Compact(mortonCode >> 1)

def DecodeMorton2DY(mortonCode):
    return Compact(mortonCode)
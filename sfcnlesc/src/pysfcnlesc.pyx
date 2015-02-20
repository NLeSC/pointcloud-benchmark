cpdef getranges(str wkt, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels, int mergeconsecutive, int maximumranges):
    ranges = getRangesWKT(wkt, minx, miny, maxx, maxy, scalex, scaley, offsetx, offsety, numberlevels, mergeconsecutive, maximumranges)
    oranges = []
    for i in range(len(ranges) / 2):
        oranges.append((int(ranges[2*i]),int(ranges[(2*i)+1])))
    return oranges


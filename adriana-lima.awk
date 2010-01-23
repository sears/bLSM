#! /usr/bin/awk -f

BEGIN{

    READ_SLA = 500;
    WRITE_SLA = 750;
    
    readcnt = 0;
    writecnt = 0;

    wlat_tot = 0;
    wlat_max = 0;
    wlat_sqtot = 0;
    wlat_slafail = 0;

    DIST_BUCKET_LENGTH = 100;
    DIST_BUCKET_COUNT = 20;
    for(i=1; i<=DIST_BUCKET_COUNT; i++)
    {
        rlat_dist[i] = 0;
        wlat_dist[i] = 0;
    }
    

    rlat_tot = 0;
    rlat_max = 0;
    rlat_sqtot = 0;
    rlat_slafail = 0;

    printf("READ SLA:\t%d\n", READ_SLA);
    printf("WRITE SLA:\t%d\n", WRITE_SLA);
    printf("\n");
    
}

/INFO - doRead()/ { readcnt = readcnt + 1;

    split(substr($0, match($0, "latency:")+ length("latency:")+1), tmp_arr, " ");
    #printf("%d\n", strtonum(tmp_arr[1]));

    lat_val = strtonum(tmp_arr[1]);

    dist_index = int(lat_val / DIST_BUCKET_LENGTH) + 1;
    if(dist_index > DIST_BUCKET_COUNT)
        dist_index = DIST_BUCKET_COUNT;
    rlat_dist[dist_index]++;

    rlat_tot = rlat_tot + lat_val;

    rlat_sqtot = rlat_sqtot + lat_val*lat_val;
    
    if(lat_val > rlat_max)
        rlat_max = lat_val;

    if(lat_val > READ_SLA)
        rlat_slafail = rlat_slafail + 1;
    
}


/INFO - doInsert()/ { writecnt = writecnt + 1;

    split(substr($0, match($0, "latency:")+ length("latency:")+1), tmp_arr, " ");

    lat_val = tmp_arr[1];
    
    if(index(tmp_arr[1], ",")!= 0)
        lat_val = substr(tmp_arr[1],1,index(tmp_arr[1],",")-1);
    
    #printf("%d\n", strtonum(lat_val));
    lat_val = strtonum(lat_val);

    dist_index = int(lat_val / DIST_BUCKET_LENGTH) + 1;
    if(dist_index > DIST_BUCKET_COUNT)
        dist_index = DIST_BUCKET_COUNT;
    wlat_dist[dist_index]++;
    
    wlat_tot = wlat_tot + lat_val;

    wlat_sqtot = wlat_sqtot + lat_val*lat_val;

    if(lat_val > wlat_max)
        wlat_max = lat_val;

    if(lat_val > WRITE_SLA)
        wlat_slafail = wlat_slafail + 1;
    

}


END{

    printf("R/W ratio:\t%.2f\n", strtonum(readcnt) / strtonum(writecnt));

    printf("\n");

    printf("#reads:\t%d\n",readcnt);
    if(strtonum(readcnt) != 0)
    {
        printf("avg read latency:\t%.2f\n", (rlat_tot / readcnt));
        printf("var read latency:\t%.2f\n", (rlat_sqtot/readcnt) - (rlat_tot/readcnt)*(rlat_tot/readcnt));
        printf("max read latency:\t%.2f\n", rlat_max);
        printf("read SLA fail:\t%d\n", rlat_slafail);

        printf("\nREAD LATENCY DISTRIBUTION\n");
        for(i=1; i<DIST_BUCKET_COUNT; i++)
            printf("\t%d - %d:\t%d\n", (i-1)*DIST_BUCKET_LENGTH, i*DIST_BUCKET_LENGTH-1, rlat_dist[i]);
        printf("\t%d - Inf:\t%d\n", (i-1)*DIST_BUCKET_LENGTH, rlat_dist[i]);
    }
    
    printf("\n");
    
    printf("#writes:\t%d\n",writecnt);
    if(strtonum(writecnt) != 0)
    {
        printf("avg write latency:\t%.2f\n", (wlat_tot / writecnt));
        printf("var write latency:\t%.2f\n", (wlat_sqtot/writecnt) - (wlat_tot/writecnt)*(wlat_tot/writecnt));
        printf("max write latency:\t%.2f\n", wlat_max);
        printf("write SLA fail:\t%d\n", wlat_slafail);

        printf("\nWRITE LATENCY DISTRIBUTION\n");
        for(i=1; i<DIST_BUCKET_COUNT; i++)
            printf("\t%d - %d:\t%d\n", (i-1)*DIST_BUCKET_LENGTH, i*DIST_BUCKET_LENGTH-1, wlat_dist[i]);
        printf("\t%d - Inf:\t%d\n", (i-1)*DIST_BUCKET_LENGTH, wlat_dist[i]);
    }

    
}


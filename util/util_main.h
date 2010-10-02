/*
 * util_open_conn.h
 *
 *  Created on: Mar 1, 2010
 *      Author: sears
 */

#ifndef UTIL_MAIN_H_
#define UTIL_MAIN_H_

logstore_handle_t * util_open_conn(int argc, char * argv[]) {
	bool ok = true;
	int svrport = 32432;
	const char * svrname = "localhost";
	if(argc == 3) {
		svrport = atoi(argv[2]);
	}
	if(argc == 2 || argc == 3) {
		svrname = argv[1];
	}
	if(!ok || argc > 3) {
		usage(argv); exit(1);
	}

    logstore_handle_t * l = logstore_client_open(svrname, svrport, 100);

    if(l == NULL) { perror("Couldn't open connection"); exit(2); }

	return l;
}


#endif /* UTIL_MAIN_H_ */

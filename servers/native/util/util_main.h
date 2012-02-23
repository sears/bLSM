/*
 * util_open_conn.h
 *
 * Copyright 2010-2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

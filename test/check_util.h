/*
 * check_util.h
 *
 *  Created on: Jan 25, 2010
 *      Author: sears
 */

#ifndef CHECK_UTIL_H_
#define CHECK_UTIL_H_

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

bool mycmp(const std::string & k1,const std::string & k2)
{
    //for char* ending with \0
    return strcmp(k1.c_str(),k2.c_str()) < 0;

    //for int32_t
    //printf("%d\t%d\n",(*((int32_t*)k1)) ,(*((int32_t*)k2)));
    //return (*((int32_t*)k1)) <= (*((int32_t*)k2));
}

//must be given a sorted array
void removeduplicates(std::vector<std::string> *arr)
{

    for(int i=arr->size()-1; i>0; i--)
    {
        if(! (mycmp((*arr)[i], (*arr)[i-1]) || mycmp((*arr)[i-1], (*arr)[i])))
            arr->erase(arr->begin()+i);

    }

}

//must be given a sorted array
// XXX probably don't need two copies of this function.
void removeduplicates(std::vector<std::string> &arr)
{

    for(int i=arr.size()-1; i>0; i--)
    {
        if(! (mycmp(arr[i], arr[i-1]) || mycmp(arr[i-1], arr[i])))
            arr.erase(arr.begin()+i);

    }

}

void getnextdata(std::string &data, int avg_len)
{
    int str_len = (rand()%(avg_len*2)) + 3;

    data = std::string(str_len, rand()%10+48);
    /*
    char *rc = (char*)malloc(str_len);

    for(int i=0; i<str_len-1; i++)
        rc[i] = rand()%10+48;

    rc[str_len-1]='\0';
    data = std::string(rc);

    free(rc);
    */

}

void preprandstr(int count, std::vector<std::string> *arr, int avg_len=50, bool duplicates_allowed=false)
{

    for ( int j=0; j<count; j++)
    {
        int str_len = (rand()%(avg_len*2)) + 3;

        char *rc = (char*)malloc(str_len);

        for(int i=0; i<str_len-1; i++)
            rc[i] = rand()%10+48;

        rc[str_len-1]='\0';
        std::string str(rc);

        //make sure there is no duplicate key
        if(!duplicates_allowed)
        {
            bool dup = false;
            for(int i=0; i<j; i++)
                if(! (mycmp((*arr)[i], str) || mycmp(str, (*arr)[i])))
                {
                    dup=true;
                    break;
                }
            if(dup)
            {
                j--;
                continue;
            }
        }


        //printf("keylen-%d\t%d\t%s\n", str_len, str.length(),rc);
        free(rc);

        arr->push_back(str);

    }

}


void preprandstr(int count, std::vector<std::string> &arr, int avg_len=50, bool duplicates_allowed=false)
{

    for ( int j=0; j<count; j++)
    {
        int str_len = (rand()%(avg_len*2)) + 3;

        char *rc = (char*)malloc(str_len);

        for(int i=0; i<str_len-1; i++)
            rc[i] = rand()%10+48;

        rc[str_len-1]='\0';
        std::string str(rc);

        //make sure there is no duplicate key
        if(!duplicates_allowed)
        {
            bool dup = false;
            for(int i=0; i<j; i++)
                if(! (mycmp(arr[i], str) || mycmp(str, arr[i])))
                {
                    dup=true;
                    break;
                }
            if(dup)
            {
                j--;
                continue;
            }
        }


        //printf("keylen-%d\t%d\t%s\n", str_len, str.length(),rc);
        free(rc);

        arr.push_back(str);

    }

}

datatuple * sendTuple(std::string & servername, int serverport, uint8_t opcode,  datatuple &tuple)
{
    struct sockaddr_in serveraddr;
    struct hostent *server;

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0)
    {
        printf("ERROR opening socket.\n");
        return 0;
    }

    server = gethostbyname(servername.c_str());
    if (server == NULL) {
        fprintf(stderr,"ERROR, no such host as %s\n", servername.c_str());
        exit(0);
    }

    /* build the server's Internet address */
    bzero((char *) &serveraddr, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    bcopy((char *)server->h_addr,
	  (char *)&serveraddr.sin_addr.s_addr, server->h_length);
    serveraddr.sin_port = htons(serverport);

    /* connect: create a connection with the server */
    if (connect(sockfd, (sockaddr*) &serveraddr, sizeof(serveraddr)) < 0)
    {
        printf("ERROR connecting\n");
        return 0;
    }


    //send the opcode
    int n = write(sockfd, (byte*) &opcode, sizeof(uint8_t));
    assert(n == sizeof(uint8_t));

    //send the tuple
    n = write(sockfd, (byte*) tuple.keylen, sizeof(uint32_t));
    assert( n == sizeof(uint32_t));

    n = write(sockfd, (byte*) tuple.datalen, sizeof(uint32_t));
    assert( n == sizeof(uint32_t));

    logserver::writetosocket(sockfd, (byte*) tuple.key, *tuple.keylen);
    if(!tuple.isDelete() && *tuple.datalen != 0)
        logserver::writetosocket(sockfd, (byte*) tuple.data, *tuple.datalen);

    //read the reply code
    uint8_t rcode;
    n = read(sockfd, (byte*) &rcode, sizeof(uint8_t));

    if(rcode == logserver::OP_SENDING_TUPLE)
    {
        datatuple *rcvdtuple = (datatuple*)malloc(sizeof(datatuple));
        //read the keylen
        rcvdtuple->keylen = (uint32_t*) malloc(sizeof(uint32_t));
        n = read(sockfd, (byte*) rcvdtuple->keylen, sizeof(uint32_t));
        assert(n == sizeof(uint32_t));
        //read the datalen
        rcvdtuple->datalen = (uint32_t*) malloc(sizeof(uint32_t));
        n = read(sockfd, (byte*) rcvdtuple->datalen, sizeof(uint32_t));
        assert(n == sizeof(uint32_t));
        //read key
        rcvdtuple->key = (byte*) malloc(*rcvdtuple->keylen);
        logserver::readfromsocket(sockfd, (byte*) rcvdtuple->key, *rcvdtuple->keylen);
        if(!rcvdtuple->isDelete())
        {
            //read key
            rcvdtuple->data = (byte*) malloc(*rcvdtuple->datalen);
            logserver::readfromsocket(sockfd, (byte*) rcvdtuple->data, *rcvdtuple->datalen);
        }

        close(sockfd);
        return rcvdtuple;
    }
    else
        assert(rcode == logserver::OP_SUCCESS);

    close(sockfd);
    return 0;
}



#endif /* CHECK_UTIL_H_ */

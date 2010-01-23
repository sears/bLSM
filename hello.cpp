
#include <string>
#include <string.h>
#include <iostream> 
#include<stasis/transactional.h>

typedef unsigned char uchar;
typedef struct datatuple
{

  typedef byte* key_t;
  typedef byte* data_t;
  uint32_t keylen;
  uint32_t datalen;
  key_t key;
  data_t data;
  

};

int main(int argc, char** argv) {

bool * m1 = new bool(false);
std::cout << *m1 << std::endl;

  datatuple t;
  std::cout << "size of datatuple:\t" << sizeof(datatuple) << std::endl;

  t.key = (datatuple::key_t) malloc(10);
  const char * str = "12345678";
  strcpy((char*)t.key, (str));

  t.keylen = strlen((char*)t.key);

  t.data = (datatuple::data_t) malloc(10);
  const char * str2 = "1234567";
  strcpy((char*)t.data, (str2));

  t.datalen = strlen((char*)t.data);

  std::cout << "size of datatuple:\t" << sizeof(datatuple) << std::endl;
  std::cout << "keylen:\t" << t.keylen << 
    "\tdatalen:\t" << t.datalen << 
    "\t" << t.key << 
    "\t" << t.data <<
    std::endl;

}

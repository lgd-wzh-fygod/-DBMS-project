#ifndef DATA_PAGE
#define DATA_PAGE
#define DATA_PAGE_SLOT_NUM 32
#include <vector>
using namespace std;
// use pm_address to locate the data in the page

// uncompressed page format design to store the buckets of PmEHash
// one slot stores one bucket of PmEHash

typedef struct kv
{
	uint64_t key;
	uint64_t value;
} kv;

typedef struct pm_bucket
{
	uint64_t local_depth;				//局部深度 
	uint8_t  bitmap[15 / 8 + 1];      // one bit for each slot
	kv       slot[15];                                // one slot for one kv-pair
} pm_bucket;

typedef struct data_page {
	// fixed-size record design
	uint32_t fileid;
	vector<pm_bucket> slot;
	uint8_t  bitmap[DATA_PAGE_SLOT_NUM];
	// uncompressed page format
} data_page;

#endif

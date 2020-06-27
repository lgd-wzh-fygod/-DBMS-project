#include"../include/pm_ehash.h"
#include <utility>
#include <libpmem.h>
#include <stdint.h>
#include <cmath>
#include <fstream>
#include <string>
#include <iostream>
#include <sstream>
/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
 
int Remove(string name){
	remove(name.c_str());
} 

typedef struct Catalog{
	int size;
	uint32_t Fileid[10000000];
	uint32_t Offset[10000000];
}Catalog;

typedef struct Metadata {
	uint64_t sum_of_bucket;		//桶的总数 
	uint64_t max_file_id;      // next file id that can be allocated
	uint64_t catalog_size;     // the catalog size of catalog file(amount of data entry)
	uint64_t global_depth;
} Metadata;


typedef struct Datapage {
	uint32_t fileid;
	uint8_t  bitmap[32];
	pm_bucket slot[32];
}Datapage;
 
 
PmEHash::PmEHash() {
	ifstream F;
	F.open("../data/catalog");
	if(F.is_open()){
		F.close();
		recover();
		
	}
	else{
		F.close();
		metadata = new ehash_metadata;   //完成metadata的初始化。 
		metadata->sum_of_bucket = 16;
		metadata->catalog_size = 16;
		metadata->global_depth = 4;
		metadata->max_file_id = 2;
		data_page Datapage;
		Datapage.fileid = 1;       // 完成datapage的初始化 
		for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
			Datapage.bitmap[i] = 0;
		}
		for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
			pm_bucket test;
			test.local_depth = 4;
			Datapage.slot.push_back(test);
		}
		datapage.push_back(Datapage);
		for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
			datapage[0].slot[i].bitmap[0] = 0;
			datapage[0].slot[i].bitmap[1] = 0;
		}
	//完成catalog的初始化							
		catalog.buckets_virtual_address = new pm_bucket * [DEFAULT_CATALOG_SIZE];
		for (int i = 0; i < DEFAULT_CATALOG_SIZE; ++i) {
			catalog.buckets_virtual_address[i] = &datapage[0].slot[i];
		}
		catalog.buckets_pm_address = new pm_address[DEFAULT_CATALOG_SIZE];
		for (int i = 0; i < DEFAULT_CATALOG_SIZE; ++i) {
			catalog.buckets_pm_address[i].fileId = Datapage.fileid;
			catalog.buckets_pm_address[i].offset = i * sizeof(pm_bucket);
		}

		for (int i = DEFAULT_CATALOG_SIZE; i < DATA_PAGE_SLOT_NUM; ++i) {
			free_list.push(&datapage[0].slot[i]);
		}

		for (int i = 0; i < DEFAULT_CATALOG_SIZE; ++i) {
			vAddr2pmAddr.insert(pair<pm_bucket*, pm_address>(&datapage[0].slot[i], catalog.buckets_pm_address[i]));
			pmAddr2vAddr.insert(pair<pm_address, pm_bucket*>(catalog.buckets_pm_address[i], &datapage[0].slot[i]));
		}
	}
}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL
 * @return: NULL
 */
PmEHash::~PmEHash() {
	//读取catalog到文件中 
	size_t catalog_size;
	int is_pmem_c;
	Catalog* tCatalog = (Catalog*)pmem_map_file("../data/catalog",sizeof(Catalog),PMEM_FILE_CREATE,0777,&catalog_size,&is_pmem_c);
	tCatalog->size = metadata->catalog_size;
	for(int i = 0; i < metadata->catalog_size; ++i) {
		tCatalog->Fileid[i] = catalog.buckets_pm_address[i].fileId;
		tCatalog->Offset[i] = catalog.buckets_pm_address[i].offset;
	}
	
	pmem_persist(tCatalog, catalog_size);
	pmem_unmap(tCatalog, catalog_size);

	//读取metadata到文件中 
	size_t metadata_size;
	int is_pmem_m;
	Metadata * tMetadata =(Metadata*) pmem_map_file("../data/metadata", sizeof(Metadata), PMEM_FILE_CREATE, 0777, &metadata_size, &is_pmem_m);
	tMetadata->sum_of_bucket = metadata->sum_of_bucket;
	tMetadata->global_depth = metadata->global_depth;
	tMetadata->max_file_id = metadata->max_file_id;
	tMetadata->catalog_size = metadata->catalog_size;
	pmem_persist(tMetadata, metadata_size);
	pmem_unmap(tMetadata, metadata_size);
	
	//读取datapage到文件中 
	for (int i = 1; i <= datapage.size(); ++i) {
		string temp_s;
		stringstream stream;
		stream << "../data/";
		stream << i;
		stream >> temp_s;
		const char* name = temp_s.c_str();
		size_t datapage_size;
		int is_pmem_d;
		Datapage * tdatapage =(Datapage*) pmem_map_file(name, sizeof(Datapage), PMEM_FILE_CREATE, 0777, &datapage_size, &is_pmem_d);
		tdatapage->fileid = datapage[i - 1].fileid;
		for (int j = 0; j < 32; ++j) {
			tdatapage->bitmap[j] = datapage[i - 1].bitmap[j];
		}
		for (int j = 0; j < datapage[i - 1].slot.size(); ++j) {
			tdatapage->slot[j].bitmap[0] = datapage[i - 1].slot[j].bitmap[0];
			tdatapage->slot[j].bitmap[1] = datapage[i - 1].slot[j].bitmap[1];
			tdatapage->slot[j].local_depth = datapage[i - 1].slot[j].local_depth;
			for (int k = 0; k < 15; ++k) {
				tdatapage->slot[j].slot[k].key = datapage[i - 1].slot[j].slot[k].key;
				tdatapage->slot[j].slot[k].value = datapage[i - 1].slot[j].slot[k].value;
			}
		}
		pmem_persist(tdatapage, datapage_size);
		pmem_unmap(tdatapage, datapage_size);
	} 
	
}

/**
 * @description: 插入新的键值对，并将相应位置上的位图置1
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
	//先查找，如果有这个键值就不插入
	uint64_t value;
	if (search(new_kv_pair.key, value) == 0) return -1;
	//插入
	pm_bucket* bucket = getFreeBucket(new_kv_pair.key);
	kv* freePlace = getFreeKvSlot(bucket);
	*freePlace = new_kv_pair;
	//位图置1
	int position = (freePlace - bucket->slot);
	if (position <= 7) bucket->bitmap[0] += pow(2, 7 - position);
	else
		bucket->bitmap[1] += pow(2, 15 - position);
	return 0;
}

/**
 * @description: 删除具有目标键的键值对数据，不直接将数据置0，而是将相应位图置0即可
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
	uint64_t bucket_id = hashFunc(key);
	pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
	
	//找到该数据在桶中的位置，记录在position中
	int bits;
	int number;
	bits = bucket->bitmap[1] > 0 ? 16 : 8;
	number = bucket->bitmap[1] > 0 ? bucket->bitmap[1] : bucket->bitmap[0];
	if (number == 0 && bits == 8) bits = 0;
	else if (number == 0 && bits == 16) bits = 8;
	else {
		while (number % 2 != 1) {
			bits--;
			number /= 2;
		}
	}
	int position = -1;
	for (int i = 0; i < bits; i++) {
		uint8_t num = (i <= 7 ? bucket->bitmap[0] : bucket->bitmap[1]);
		int b = (i <= 7 ? 7 : 15);	
		if ((num >> (b-i))%2 == 1 && bucket->slot[i].key == key) {
			position = i;
			break;
		}
	}	
	
//没有找到 
	if (position == -1) return -1; 	
	
	//位图置0
	if (position <= 7) bucket->bitmap[0] -= pow(2, 7 - position);
	else
		bucket->bitmap[0] -= pow(2, 15 - position);
	if (bucket->bitmap[0] == 0 && bucket->bitmap[1] == 0) mergeBucket(bucket_id);
	return 0;
}
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
	uint64_t bucket_id = hashFunc(kv_pair.key);
	pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
	
	//算出slot中数据个数 
	int bits;
	int number;    
	bits = bucket->bitmap[1] > 0 ? 16 : 8;
	number = bucket->bitmap[1] > 0 ? bucket->bitmap[1] : bucket->bitmap[0];
	if (number == 0 && bits == 8) bits = 0;
	else if (number == 0 && bits == 16) bits = 8;
	else {
		while (number % 2 != 1) {
			bits--;
			number /= 2;
		}
	}
	
	//查找并更新  
	for (int i = 0; i < bits; i++) {
		uint8_t num = (i <= 7 ? bucket->bitmap[0] : bucket->bitmap[1]);
		int b = (i <= 7 ? 7 : 15);
		if ((num >> (b-i))%2 == 1 && bucket->slot[i].key == kv_pair.key) {
			bucket->slot[i].value = kv_pair.value;
			return 0;
		}
	}
	return -1;	
}
/**
 * @description: 查找目标键值对数据，将返回值放在参数里的引用类型进行返回
 * @param uint64_t: 查询的目标键
 * @param uint64_t&: 查询成功后返回的目标值
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist)
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {
	uint64_t bucket_id = hashFunc(key);
	pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
	
	//算出slot中数据个数
	int bits;
	int number;  
	bits = bucket->bitmap[1] > 0 ? 16 : 8;
	number = bucket->bitmap[1] > 0 ? bucket->bitmap[1] : bucket->bitmap[0];
	if (number == 0 && bits == 8) bits = 0;
	else if (number == 0 && bits == 16) bits = 8;
	else {
		while (number % 2 != 1) {
			bits--;
			number /= 2;
		}
	}
	
	//查找
	for (int i = 0; i < bits; i++) {
		uint8_t num = (i <= 7 ? bucket->bitmap[0] : bucket->bitmap[1]);
		int b = (i <= 7 ? 7 : 15);
		if ((num >> (b-i))%2 == 1 && bucket->slot[i].key == key) {
			return_val = bucket->slot[i].value;
			return 0;
		}
	}
	return -1;
}

/**
 * @description: 用于对输入的键产生哈希值，然后取模求桶号(自己挑选合适的哈希函数处理)
 * @param uint64_t: 输入的键
 * @return: 返回键所属的桶号
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
	int tmp;
	int pos;
	for (int i = 4; i <= metadata->global_depth; ++i) {
		tmp = (1 << i);
		pos = key % tmp;
		if (catalog.buckets_virtual_address[pos]->local_depth == i)return pos;
	}
}

/**
 * @description: 获得供插入的空闲的桶，无空闲桶则先分裂桶然后再返回空闲的桶
 * @param uint64_t: 带插入的键
 * @return: 空闲桶的虚拟地址
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
	uint64_t Key = hashFunc(key);
	pm_bucket* temp_bucket = catalog.buckets_virtual_address[Key];
	while ((*temp_bucket).bitmap[0] == 255 && (*temp_bucket).bitmap[1] == 254) {
		splitBucket(Key);
		uint64_t temp = 1;
		temp = (temp << (temp_bucket->local_depth-1));
		temp_bucket = catalog.buckets_virtual_address[Key];
	}
	Key = hashFunc(key); 
	return catalog.buckets_virtual_address[Key];

}

pm_bucket* PmEHash::getNewBucket()
{
	return nullptr;
}

/**
 * @description: 获得空闲桶内第一个空闲的位置供键值对插入
 * @param pm_bucket* bucket
 * @return: 空闲键值对位置的虚拟地址
 */
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
		//算出slot中数据个数    
	int bits;
	int number;
	bits = bucket->bitmap[1] > 0 ? 16 : 8;
	number = bucket->bitmap[1] > 0 ? bucket->bitmap[1] : bucket->bitmap[0];
	if (number == 0 && bits == 8) return &bucket->slot[0];
	if (number == 0 && bits == 16) return &bucket->slot[8];
	while (number % 2 != 1) {
		bits--;
		number = number / 2;
	}
	for (int i = 0; i < bits; i ++) {
		uint8_t num = (i <= 7 ? bucket->bitmap[0] : bucket->bitmap[1]);
		int b = (i <= 7 ? 7 : 15);
		if ((num >> (b-i))%2 == 0) {
			return &bucket->slot[i]; 
		}
	}
	return &bucket->slot[bits];
}


/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {//脫脨脦脢脤芒 
	
	metadata->sum_of_bucket++;
	int offset = metadata->sum_of_bucket % DATA_PAGE_SLOT_NUM;
	uint64_t temp = catalog.buckets_virtual_address[bucket_id]->local_depth;
	if (temp == metadata->global_depth) {
		extendCatalog();
	}
	catalog.buckets_virtual_address[bucket_id]->local_depth++;
	uint64_t num = (1 << temp);
	uint64_t page_num = bucket_id + num;//新的桶号 
	pm_address new_address = catalog.buckets_pm_address[page_num];
	catalog.buckets_virtual_address[page_num] = (pm_bucket*)getFreeSlot(new_address);
	catalog.buckets_pm_address[page_num].fileId = metadata->max_file_id - 1;
	catalog.buckets_pm_address[page_num].offset = (offset - 1) * sizeof(pm_bucket);
	vAddr2pmAddr.insert(pair<pm_bucket*, pm_address>(catalog.buckets_virtual_address[page_num], new_address));
	pmAddr2vAddr.insert(pair<pm_address, pm_bucket*>(new_address, catalog.buckets_virtual_address[page_num]));
	catalog.buckets_virtual_address[bucket_id]->bitmap[0] = 0;
	catalog.buckets_virtual_address[bucket_id]->bitmap[1] = 0;
	catalog.buckets_virtual_address[page_num]->local_depth =temp +1;
	for (int i = 0; i < 15; ++i) {
		kv temp = catalog.buckets_virtual_address[bucket_id]->slot[i];
		insert(temp);
	}
}

/**
 * @description: 桶空后，回收桶的空间，并设置相应目录项指针
 * @param uint64_t: 桶号
 * @return: NULL
 */
void PmEHash::mergeBucket(uint64_t bucket_id) {
	if (bucket_id >= 0 && bucket_id <= 15) return;
	else {
		int position;
		pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
//判断该桶是否分裂过
		bool status = false;
		uint64_t depth = bucket->local_depth;
		while(1) {
			if (bucket_id >= pow(2,depth-1) && bucket_id < pow(2,depth)) break;
			depth--;
			status = true;
		}
//已经分裂过，回收分裂的桶
		if (status) {
			uint64_t next_bucket_id = bucket_id + pow(2,bucket->local_depth-1);
			pm_bucket *next_bucket = catalog.buckets_virtual_address[next_bucket_id];
			bucket->local_depth--;
//算出slot中数据个数 
			int bits;
			int number;    
			bits = next_bucket->bitmap[1] > 0 ? 16 : 8;
			number = next_bucket->bitmap[1] > 0 ? next_bucket->bitmap[1] : next_bucket->bitmap[0];
			if (number == 0 && bits == 8) bits = 0;
			else if (number == 0 && bits == 16) bits = 8;
			else {
				while (number % 2 != 1) {
					bits--;
					number /= 2;
				}
			}
				//重新插入 
			for (int i = 0; i < bits; i++) {
				uint8_t num = (i <= 7 ? next_bucket->bitmap[0] : next_bucket->bitmap[1]);
				int b = (i <= 7 ? 7 : 15);
				if ((num >> (b-i))%2 == 1) {
					insert(next_bucket->slot[i]);
				}
			}
			//next_bucket中位图置0
			next_bucket->bitmap[0] = next_bucket->bitmap[1] = 0; 
			catalog.buckets_pm_address[next_bucket_id].fileId = 0;
			catalog.buckets_virtual_address[next_bucket_id] = NULL;
			position = catalog.buckets_pm_address[next_bucket_id].offset / sizeof(pm_bucket); 
		}
	
//没有分裂过，回收该桶
		else {
			uint64_t last_bucket_id = bucket_id - pow(2,bucket->local_depth-1);
			pm_bucket *last_bucket = catalog.buckets_virtual_address[last_bucket_id];
//该桶与分裂它的桶局部深度相同，合并该桶 
			if (bucket->local_depth == last_bucket->local_depth) {
				last_bucket->local_depth--;
				catalog.buckets_pm_address[bucket_id].fileId = 0;
				catalog.buckets_virtual_address[bucket_id] = NULL;
				position = catalog.buckets_pm_address[bucket_id].offset / sizeof(pm_bucket);
			}	
//该桶与分裂它的桶局部深度不同，保留该桶
			else
				position = -1;
		}
//删除映射表里桶的地址
		
//删除数据页中的桶
		if (position != -1) {
			queue<pm_bucket*> list;
			list.push(&datapage[catalog.buckets_pm_address[bucket_id].fileId - 1].slot[position]);
			pm_bucket *temp;
			for (int i = 0; i < free_list.size(); i++) {
				temp = free_list.front();
				free_list.pop();
				list.push(temp);
			}
			free_list = list;
			datapage[catalog.buckets_pm_address[bucket_id].fileId - 1].bitmap[position] = 0;
			metadata->sum_of_bucket--;
		}
		
	}
}

/**
 * @description: 对目录进行倍增，需要重新生成新的目录文件并复制旧值，然后删除旧的目录文件
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {
	int size = metadata->catalog_size * 2;
	ehash_catalog new_catalog;
	new_catalog.buckets_pm_address = new pm_address[size];
	new_catalog.buckets_virtual_address = new pm_bucket * [size];
	
//将旧目录的数据拷贝过去
	for (int i = 0; i < size / 2; i++) {
		new_catalog.buckets_pm_address[i] = catalog.buckets_pm_address[i];
		new_catalog.buckets_virtual_address[i] = catalog.buckets_virtual_address[i];
	}
	for (int i = size/2; i < size; i++){
		new_catalog.buckets_virtual_address[i] = NULL;
		new_catalog.buckets_pm_address[i].fileId = 0;
		new_catalog.buckets_pm_address[i].offset = 0; 
	}
	
	//删除旧目录
	delete[]catalog.buckets_pm_address;
	delete[]catalog.buckets_virtual_address;
	catalog = new_catalog;
	
	//更新metadata 
	metadata->global_depth++;
	metadata->catalog_size *= 2;
}

/**
 * @description: 获得一个可用的数据页的新槽位供哈希桶使用，如果没有则先申请新的数据页
 * @param pm_address&: 新槽位的持久化文件地址，作为引用参数返回
 * @return: 新槽位的虚拟地址
 */
void* PmEHash::getFreeSlot(pm_address& new_address) {
	if (free_list.size() == 0) {
		allocNewPage();
	}

	pm_bucket* temp = free_list.front();
	free_list.pop();
	return temp;
}

/**
 * @description: 申请新的数据页文件，并把所有新产生的空闲槽的地址放入free_list等数据结构中
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {
	data_page temp_page;
	temp_page.fileid = datapage.size() + 1;
	datapage.push_back(temp_page);
	for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
		datapage[datapage.size() - 1].bitmap[i] = 0;
	}
	
	for (int i = 0; i < 32; ++i) {
		pm_bucket temp_bucket;
		temp_bucket.bitmap[0] = 0;
		temp_bucket.bitmap[1] = 0;
		datapage[datapage.size() - 1].slot.push_back(temp_bucket);
	}
	
	for (int i = 0; i < 32; ++i) {
		free_list.push(&(datapage[datapage.size() - 1].slot[i]));
	}
	metadata->max_file_id++;
}

/**
 * @description: 读取旧数据文件重新载入哈希，恢复哈希关闭前的状态
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {
	size_t catalog_size;
	int is_pmem_c;
	Catalog* tCatalog = (Catalog*)pmem_map_file("../data/catalog", sizeof(Catalog), PMEM_FILE_CREATE, 0777, &catalog_size, &is_pmem_c);
	int catasize = tCatalog->size;
	catalog.buckets_pm_address = new pm_address[catasize];
	catalog.buckets_virtual_address = new pm_bucket * [catasize];
	for (int i = 0; i < catasize; ++i) {
		catalog.buckets_pm_address[i].fileId = tCatalog->Fileid[i];
		catalog.buckets_pm_address[i].offset = tCatalog->Offset[i];
	}
	pmem_unmap(tCatalog, catalog_size);
	
	
	size_t metadata_size;
	int is_pmem_m;
	Metadata* tMetadata = (Metadata*)pmem_map_file("../data/metadata", sizeof(Metadata), PMEM_FILE_CREATE, 0777, &metadata_size, &is_pmem_m);
	metadata->catalog_size = tMetadata->catalog_size;
	metadata->global_depth = tMetadata->global_depth;
	metadata->max_file_id = tMetadata->max_file_id;
	metadata->sum_of_bucket = tMetadata->sum_of_bucket;
	pmem_unmap(tMetadata, metadata_size);
	

	for (int i = 1; i < metadata->max_file_id ; ++i) {
		size_t pagedata_size;
		int is_pmem_d;
		string temp_s;
		stringstream stream;
		stream << "../data/";
		stream << i;
		stream >> temp_s;
		const char* name = temp_s.c_str();
		Datapage* tDatapage = (Datapage*)pmem_map_file(name, sizeof(Datapage), PMEM_FILE_CREATE, 0777, &pagedata_size, &is_pmem_d);
		data_page testdatapage;
		for(int j = 0;j<32;++j){
			pm_bucket temp_b;
			testdatapage.slot.push_back(temp_b);
		}
		 
		testdatapage.fileid = tDatapage->fileid;
		for (int k = 0; k < 32; ++k) {
			testdatapage.bitmap[k] = tDatapage->bitmap[k];
		}
		
		for (int j = 0; j < 32; ++j) {
			testdatapage.slot[j].bitmap[0] = tDatapage->slot[j].bitmap[0];
			testdatapage.slot[j].bitmap[1] = tDatapage->slot[j].bitmap[1];
			testdatapage.slot[j].local_depth = tDatapage->slot[j].local_depth;
			for (int k = 0; k < 15; ++k) {
				testdatapage.slot[j].slot[k].key = tDatapage->slot[j].slot[k].key;
				testdatapage.slot[j].slot[k].value = tDatapage->slot[j].slot[k].value;
			}
		}
		datapage.push_back(testdatapage);
		pmem_unmap(tDatapage, pagedata_size);
	}
	mapAllPage();
}

/**
 * @description: 重启时，将所有数据页进行内存映射，设置地址间的映射关系，空闲的和使用的槽位都需要设置
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {
	for (int i = 0; i < metadata->catalog_size; ++i) {
		if(catalog.buckets_pm_address[i].fileId !=0){
			pm_bucket * temp  = &datapage[catalog.buckets_pm_address[i].fileId-1].slot.front();
			catalog.buckets_virtual_address[i] = temp + catalog.buckets_pm_address[i].offset/256;
		}
	}
	cout << endl;
	cout <<endl;
}

/**
 * @description: 删除PmEHash对象所有数据页，目录和元数据文件，主要供gtest使用。即清空所有可扩展哈希的文件数据，不止是内存上的
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {
	Remove("catalog");
	Remove("metadata");
	Remove("1");
}


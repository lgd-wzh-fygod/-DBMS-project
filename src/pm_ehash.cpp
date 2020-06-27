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
	uint64_t sum_of_bucket;		//Ͱ������ 
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
		metadata = new ehash_metadata;   //���metadata�ĳ�ʼ���� 
		metadata->sum_of_bucket = 16;
		metadata->catalog_size = 16;
		metadata->global_depth = 4;
		metadata->max_file_id = 2;
		data_page Datapage;
		Datapage.fileid = 1;       // ���datapage�ĳ�ʼ�� 
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
	//���catalog�ĳ�ʼ��							
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
	//��ȡcatalog���ļ��� 
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

	//��ȡmetadata���ļ��� 
	size_t metadata_size;
	int is_pmem_m;
	Metadata * tMetadata =(Metadata*) pmem_map_file("../data/metadata", sizeof(Metadata), PMEM_FILE_CREATE, 0777, &metadata_size, &is_pmem_m);
	tMetadata->sum_of_bucket = metadata->sum_of_bucket;
	tMetadata->global_depth = metadata->global_depth;
	tMetadata->max_file_id = metadata->max_file_id;
	tMetadata->catalog_size = metadata->catalog_size;
	pmem_persist(tMetadata, metadata_size);
	pmem_unmap(tMetadata, metadata_size);
	
	//��ȡdatapage���ļ��� 
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
 * @description: �����µļ�ֵ�ԣ�������Ӧλ���ϵ�λͼ��1
 * @param kv: ����ļ�ֵ��
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
	//�Ȳ��ң�����������ֵ�Ͳ�����
	uint64_t value;
	if (search(new_kv_pair.key, value) == 0) return -1;
	//����
	pm_bucket* bucket = getFreeBucket(new_kv_pair.key);
	kv* freePlace = getFreeKvSlot(bucket);
	*freePlace = new_kv_pair;
	//λͼ��1
	int position = (freePlace - bucket->slot);
	if (position <= 7) bucket->bitmap[0] += pow(2, 7 - position);
	else
		bucket->bitmap[1] += pow(2, 15 - position);
	return 0;
}

/**
 * @description: ɾ������Ŀ����ļ�ֵ�����ݣ���ֱ�ӽ�������0�����ǽ���Ӧλͼ��0����
 * @param uint64_t: Ҫɾ����Ŀ���ֵ�Եļ�
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
	uint64_t bucket_id = hashFunc(key);
	pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
	
	//�ҵ���������Ͱ�е�λ�ã���¼��position��
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
	
//û���ҵ� 
	if (position == -1) return -1; 	
	
	//λͼ��0
	if (position <= 7) bucket->bitmap[0] -= pow(2, 7 - position);
	else
		bucket->bitmap[0] -= pow(2, 15 - position);
	if (bucket->bitmap[0] == 0 && bucket->bitmap[1] == 0) mergeBucket(bucket_id);
	return 0;
}
/**
 * @description: �����ִ�ļ�ֵ�Ե�ֵ
 * @param kv: ���µļ�ֵ�ԣ���ԭ������ֵ
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
	uint64_t bucket_id = hashFunc(kv_pair.key);
	pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
	
	//���slot�����ݸ��� 
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
	
	//���Ҳ�����  
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
 * @description: ����Ŀ���ֵ�����ݣ�������ֵ���ڲ�������������ͽ��з���
 * @param uint64_t: ��ѯ��Ŀ���
 * @param uint64_t&: ��ѯ�ɹ��󷵻ص�Ŀ��ֵ
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist)
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {
	uint64_t bucket_id = hashFunc(key);
	pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
	
	//���slot�����ݸ���
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
	
	//����
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
 * @description: ���ڶ�����ļ�������ϣֵ��Ȼ��ȡģ��Ͱ��(�Լ���ѡ���ʵĹ�ϣ��������)
 * @param uint64_t: ����ļ�
 * @return: ���ؼ�������Ͱ��
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
 * @description: ��ù�����Ŀ��е�Ͱ���޿���Ͱ���ȷ���ͰȻ���ٷ��ؿ��е�Ͱ
 * @param uint64_t: ������ļ�
 * @return: ����Ͱ�������ַ
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
 * @description: ��ÿ���Ͱ�ڵ�һ�����е�λ�ù���ֵ�Բ���
 * @param pm_bucket* bucket
 * @return: ���м�ֵ��λ�õ������ַ
 */
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
		//���slot�����ݸ���    
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
 * @description: Ͱ������з��Ѳ��������ܴ���Ŀ¼�ı���
 * @param uint64_t: Ŀ��Ͱ��Ŀ¼�е����
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {//ÓÐÎÊÌâ 
	
	metadata->sum_of_bucket++;
	int offset = metadata->sum_of_bucket % DATA_PAGE_SLOT_NUM;
	uint64_t temp = catalog.buckets_virtual_address[bucket_id]->local_depth;
	if (temp == metadata->global_depth) {
		extendCatalog();
	}
	catalog.buckets_virtual_address[bucket_id]->local_depth++;
	uint64_t num = (1 << temp);
	uint64_t page_num = bucket_id + num;//�µ�Ͱ�� 
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
 * @description: Ͱ�պ󣬻���Ͱ�Ŀռ䣬��������ӦĿ¼��ָ��
 * @param uint64_t: Ͱ��
 * @return: NULL
 */
void PmEHash::mergeBucket(uint64_t bucket_id) {
	if (bucket_id >= 0 && bucket_id <= 15) return;
	else {
		int position;
		pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
//�жϸ�Ͱ�Ƿ���ѹ�
		bool status = false;
		uint64_t depth = bucket->local_depth;
		while(1) {
			if (bucket_id >= pow(2,depth-1) && bucket_id < pow(2,depth)) break;
			depth--;
			status = true;
		}
//�Ѿ����ѹ������շ��ѵ�Ͱ
		if (status) {
			uint64_t next_bucket_id = bucket_id + pow(2,bucket->local_depth-1);
			pm_bucket *next_bucket = catalog.buckets_virtual_address[next_bucket_id];
			bucket->local_depth--;
//���slot�����ݸ��� 
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
				//���²��� 
			for (int i = 0; i < bits; i++) {
				uint8_t num = (i <= 7 ? next_bucket->bitmap[0] : next_bucket->bitmap[1]);
				int b = (i <= 7 ? 7 : 15);
				if ((num >> (b-i))%2 == 1) {
					insert(next_bucket->slot[i]);
				}
			}
			//next_bucket��λͼ��0
			next_bucket->bitmap[0] = next_bucket->bitmap[1] = 0; 
			catalog.buckets_pm_address[next_bucket_id].fileId = 0;
			catalog.buckets_virtual_address[next_bucket_id] = NULL;
			position = catalog.buckets_pm_address[next_bucket_id].offset / sizeof(pm_bucket); 
		}
	
//û�з��ѹ������ո�Ͱ
		else {
			uint64_t last_bucket_id = bucket_id - pow(2,bucket->local_depth-1);
			pm_bucket *last_bucket = catalog.buckets_virtual_address[last_bucket_id];
//��Ͱ���������Ͱ�ֲ������ͬ���ϲ���Ͱ 
			if (bucket->local_depth == last_bucket->local_depth) {
				last_bucket->local_depth--;
				catalog.buckets_pm_address[bucket_id].fileId = 0;
				catalog.buckets_virtual_address[bucket_id] = NULL;
				position = catalog.buckets_pm_address[bucket_id].offset / sizeof(pm_bucket);
			}	
//��Ͱ���������Ͱ�ֲ���Ȳ�ͬ��������Ͱ
			else
				position = -1;
		}
//ɾ��ӳ�����Ͱ�ĵ�ַ
		
//ɾ������ҳ�е�Ͱ
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
 * @description: ��Ŀ¼���б�������Ҫ���������µ�Ŀ¼�ļ������ƾ�ֵ��Ȼ��ɾ���ɵ�Ŀ¼�ļ�
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {
	int size = metadata->catalog_size * 2;
	ehash_catalog new_catalog;
	new_catalog.buckets_pm_address = new pm_address[size];
	new_catalog.buckets_virtual_address = new pm_bucket * [size];
	
//����Ŀ¼�����ݿ�����ȥ
	for (int i = 0; i < size / 2; i++) {
		new_catalog.buckets_pm_address[i] = catalog.buckets_pm_address[i];
		new_catalog.buckets_virtual_address[i] = catalog.buckets_virtual_address[i];
	}
	for (int i = size/2; i < size; i++){
		new_catalog.buckets_virtual_address[i] = NULL;
		new_catalog.buckets_pm_address[i].fileId = 0;
		new_catalog.buckets_pm_address[i].offset = 0; 
	}
	
	//ɾ����Ŀ¼
	delete[]catalog.buckets_pm_address;
	delete[]catalog.buckets_virtual_address;
	catalog = new_catalog;
	
	//����metadata 
	metadata->global_depth++;
	metadata->catalog_size *= 2;
}

/**
 * @description: ���һ�����õ�����ҳ���²�λ����ϣͰʹ�ã����û�����������µ�����ҳ
 * @param pm_address&: �²�λ�ĳ־û��ļ���ַ����Ϊ���ò�������
 * @return: �²�λ�������ַ
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
 * @description: �����µ�����ҳ�ļ������������²����Ŀ��в۵ĵ�ַ����free_list�����ݽṹ��
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
 * @description: ��ȡ�������ļ����������ϣ���ָ���ϣ�ر�ǰ��״̬
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
 * @description: ����ʱ������������ҳ�����ڴ�ӳ�䣬���õ�ַ���ӳ���ϵ�����еĺ�ʹ�õĲ�λ����Ҫ����
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
 * @description: ɾ��PmEHash������������ҳ��Ŀ¼��Ԫ�����ļ�����Ҫ��gtestʹ�á���������п���չ��ϣ���ļ����ݣ���ֹ���ڴ��ϵ�
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {
	Remove("catalog");
	Remove("metadata");
	Remove("1");
}


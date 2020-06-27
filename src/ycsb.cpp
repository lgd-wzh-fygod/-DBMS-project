#include "../include/pm_ehash.h"
#include "../include/data_page.h"
#include "pm_ehash.cpp"
#include <iostream>
#include <fstream>
#include <sstream>

//将此文件放入src文件夹中 
using namespace std;
int main(){
clock_t t1 = clock();
	PmEHash A;
	ifstream f1("../workloads/220w-rw-50-50-load.txt");   //执行load操作 
	string temp;
	string s;
	stringstream ss;
	int a;
	kv t;
	double total_load = 0;
	double total_run = 0;
	double total_insert = 0;
	double total_update = 0;
	double total_search = 0;
	 
	if (f1.is_open()) {
		while(f1 >> temp) {
			if (temp == "INSERT") {
				f1 >> s;
				s = s.substr(0,8);
				ss << s;
				ss >> a;
				ss.clear();
				t.key = a;
				t.value = a;
				A.insert(t); 
				++total_load;
			}
			
		}
	}
	f1.close();
	ifstream f2("../workloads/220w-rw-50-50-run.txt"); //执行run操作 
	uint64_t b;
	clock_t t2 = clock();
	if (f2.is_open()) {
		while(f2 >> temp) {
			if (temp == "UPDATE") {
				f2 >> s;
				s = s.substr(0,8);
				ss << s;
				ss >> a;
				ss.clear();
				t.key = a;
				t.value = a;
				A.update(t); 
				++total_run;
				++total_update;
			}
			else if (temp == "READ") {
				f2 >> s;
				s = s.substr(0,8);
				ss << s;
				ss >> a;
				ss.clear();
				t.key = a;
				t.value = a;
				A.search(t.key,b);	
				++total_run;
				++total_search;
			} 
			else if (temp == "INSERT") {
				f2 >> s;
				s = s.substr(0,8);
				ss << s;
				ss >> a;
				ss.clear();
				t.key = a;
				t.value = a;
				A.insert(t);	
				++total_insert;
				++total_run;
			} 
		}
	}
	f2.close();
	clock_t t3 = clock();
	double time = (t3-t2);//输出测试的结果 
	cout << "load time  "<<(t2-t1)/CLOCKS_PER_SEC<<endl;
	cout << "run time  "<<time/CLOCKS_PER_SEC<<endl;
	cout << "sum of load operation  "<<total_load <<endl;
	cout << "sum of run operation   "<<total_run<<endl;
	cout << "proportion of operation   "<< " insert / search "<<total_insert / total_search<<endl;
	cout << "OPS   "<< (total_run /time)*1000000 <<endl;
}

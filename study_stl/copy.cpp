#include <iostream>
#include <list>
#include <vector>
#include <algorithm>
#include <deque>
#include <iterator>
#include <set>

using namespace std;
void print(int x)
{
    cout << x << " ";
}

void insertIterator(vector<int> &vec)
{
    list<int> ll;
    cout << "Test insert iterator" << endl;

    copy(vec.begin(), vec.end(), back_inserter(ll));
    for_each(ll.begin(), ll.end(), print);
    cout << endl;

    copy(vec.begin(), vec.end(), front_inserter(ll));
    for_each(ll.begin(), ll.end(), print);
    cout << endl;

    deque<int> deq;
    copy(vec.begin(), vec.end(), inserter(deq, deq.begin()));
    for_each(deq.begin(), deq.end(), print);
    cout << endl;

    set<int> ss;
    vec.push_back(4);
    unique_copy(vec.begin(), vec.end(), inserter(ss, ss.begin()));
    //for_each(ss.begin(), ss.end(), print);
    copy(ss.begin(), ss.end(), ostream_iterator<int>(cout, " "));
    cout << endl;

}

int main(int argc, char** argv)
{
    vector<int> vec;
    list<int> ll;

    for (int i = 1 ; i <= 10; ++i) {
        ll.push_back(i);
    }

    vec.resize(ll.size());
    copy(ll.begin(), ll.end(), vec.begin());
    for_each(vec.begin(), vec.end(), print);
    cout << endl;

    deque<int> deq(vec.size());

    copy(vec.begin(), vec.end(), deq.begin());
    for_each(deq.begin(), deq.end(), print);
    cout << endl;

    insertIterator(vec);

    return 0;
}

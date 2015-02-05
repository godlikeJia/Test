#include <iostream>
#include <list>
#include <algorithm>
#include <iterator>

using namespace std;

int main(int argc, char** argv)
{
    list<int> coll;
    for (int i=0; i<10; i++) {
        coll.push_back(i);
        coll.push_front(i);
    }

    copy(coll.begin(), coll.end(), ostream_iterator<int>(cout, " "));
    cout << endl;
    
    list<int>::iterator end = remove(coll.begin(), coll.end(), 3);

    copy(coll.begin(), coll.end(), ostream_iterator<int>(cout, " "));
    cout << endl;

    copy(coll.begin(), end, ostream_iterator<int>(cout, " "));
    cout << endl;

    cout << "number of removed elements: " << distance(end, coll.end()) << endl;

    //coll.erase(remove(coll.begin(), coll.end(), 3), coll.end()); // combined remove and erase
    coll.erase(end, coll.end());
    copy(coll.begin(), coll.end(), ostream_iterator<int>(cout, " "));
    cout << endl;

    return 0;
}

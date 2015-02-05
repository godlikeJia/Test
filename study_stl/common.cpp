#include <iostream>
#include <vector>
#include <algorithm>

using namespace std;
bool comp(int x, int y)
{
    return x > y ? false : true;
}

int main(int argc, char** argv)
{
    vector<int> vec;
    vec.push_back(4);
    vec.push_back(3);
    vec.push_back(2);
    vec.push_back(5);
    vec.push_back(9);
    vec.push_back(8);
    vec.push_back(6);
    vec.push_back(7);
    vec.push_back(1);

    vector<int>::iterator pos;
    pos = max_element(vec.begin(), vec.end());
    cout << "max_element: " << *pos << endl;

    pos = min_element(vec.begin(), vec.end());
    cout << "min_element: " << *pos << endl;

    pos = find(vec.begin(), vec.end(), 3);
    cout << "min_element: " << *(++pos) << endl;

    sort(vec.begin(), vec.end()-5);
    sort(vec.end()-5, vec.end(), comp);
    reverse(vec.end()-5, vec.end());

    for (pos = vec.begin(); pos != vec.end(); ++pos) {
        cout << " " << *pos;
    }
    cout << endl;


    pos = find_if(vec.begin(), vec.end(), compose_f_gx_hx(logical_or<bool>(),
                                                            bind2nd(equal_to<int>(), 3),
                                                            bind2nd(equal_to<int>(), 2))),
    if (pos != vec.end())
        cout << *pos << endl;
    return 0;
}

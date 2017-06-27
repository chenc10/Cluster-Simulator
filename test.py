from collections import OrderedDict
import json

def main():
    d = OrderedDict()
    d['0'] = {}
    d['0']['heng'] = '0heng'
    d['2'] = {}
    d['2']['heng']= '2heng'
    d['1'] = {}
    d['1']['heng'] = '1heng'
    d['16'] = {}
    d['16']['heng'] = '16heng'
    for k, v in d.iteritems():
        print k, v['heng']
    with open('test.json' , 'w') as f:
        json.dump(d, f)
    data = json.load(open('test.json'), object_pairs_hook=OrderedDict)
    for id in data:
        print data[id]['heng']


if __name__ == '__main__':
    main()
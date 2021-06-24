from kuebiko.kuebiko import Kuebiko

if __name__ == "__main__":
    k = Kuebiko()
    result = k.query("test_query.json")
    print('Returned {} articles'.format(len(result)))
import random, string;


print '''CREATE TABLE test_pk_index (
    id CHAR(40) PRIMARY KEY,
    value VARCHAR(20)
);
'''

ids = range(50000)
random.shuffle(ids);

def gen_string():
    length = random.randint(5, 20)
    letters = list(string.ascii_letters + string.digits)
    s = random.sample(letters, length)
    s = ''.join(s)
    return s

for id in ids:
    print "INSERT INTO test_pk_index VALUES ('%d', '%s');" % (id, gen_string())

print "\nQUIT;"


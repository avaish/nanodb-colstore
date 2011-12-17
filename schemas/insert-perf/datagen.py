import sys, random

word_set = set()

def import_words(filename):
    f = open(filename)
    contents = f.readlines()
    for word in contents:
        if word.find("'") != -1:
            continue

        word = word.strip().lower()
        word_set.add(word)

for filename in sys.argv[1:]:
    print >> sys.stderr, filename
    import_words(filename)

word_list = list(word_set)
for id in range(1, 200001):
    num = random.random() * 100.0
    str = ' '.join(random.sample(word_list, random.randint(3, 20)))
    if len(str) > 200:    # Truncate if necessary
        str = str[:200]
    print 'INSERT INTO insert_perf VALUES (%d, \'%s\', %g);' % (id, str, num)

    if random.random() <= 0.005:
        # Generate a range where a and b are always within 1 of each other.
        a = random.random() * 100.0
        b = a + random.random() - 0.5
        print 'DELETE FROM insert_perf WHERE num >= %g AND num <= %g;' % (min(a, b), max(a, b))

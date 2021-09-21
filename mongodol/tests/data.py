"""A module containing datas that will be used for tests"""

from mongodol.base import ID


def mk_kvs_versions(name):
    """Makes dict_kvs and tuple_kvs versions of the data referenced by name
    By kvs we mean "key value expressions", i.e. (key, value) pairs.
    A dict_kvs is one where the key and value are both dicts (e.g. ({'_id': 2}, {'name': "yoda", 'age': 800}))
    A tuple_kvs is one where the key is a tuple (e.g. ((2,), {'name': "yoda", 'age': 800}))

    mk_kvs_versions reads and writes in the globals()
    """
    global_vars = globals()
    seed_docs = global_vars[name]

    dict_kvs_1_docs = [
        ({ID: doc[ID]}, {k: doc[k] for k in doc.keys() - {ID}}) for doc in seed_docs
    ]
    global_vars[f'{name}_dict_kvs_1'] = dict_kvs_1_docs

    dict_tuple_kvs_1 = [(k[ID], tuple(v.values())) for k, v in dict_kvs_1_docs]
    global_vars[f'{name}_tuple_kvs_1'] = dict_tuple_kvs_1


# Simple with one field having a repeated value
three_simple_docs = [
    {ID: 0, 's': 'a', 'n': 1},
    {ID: 1, 's': 'b', 'n': 2},
    {ID: 2, 's': 'b', 'n': 3},
]
mk_kvs_versions('three_simple_docs')

# Schema not "perfect": Some missing fields here and there.
# Good to test no-sql cases
nums_and_lans = [
    {ID: 1, 'en': 'one', 'fr': 'un', 'sp': 'uno', 'so_far': [1]},
    {ID: 2, 'en': 'two', 'fr': 'deux', 'so_far': [1, 2]},  # sp missing
    {ID: 3, 'en': 'three', 'fr': 'trois', 'sp': 'tres', 'so_far': [1, 2, 3]},
    {ID: 4, 'en': 'four', 'fr': 'quatre', 'sp': 'cuatro', 'so_far': [1, 2, 3, 4],},
    {ID: 5, 'en': 'five', 'sp': 'cinco', 'so_far': [1, 2, 3, 4, 5],},  # fr missing
]
mk_kvs_versions('nums_and_lans')

# Stable schema
# Groupby possibilities (see number: Several unique values) -- this allows to test filtering more naturally
feature_cube = [
    {ID: 1, 'number': 6, 'color': 'red', 'dims': {'x': 2, 'y': 3}},
    {ID: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}},
    {ID: 3, 'number': 10, 'color': 'red', 'dims': {'x': 2, 'y': 5}},
    {ID: 4, 'number': 10, 'color': 'red', 'dims': {'x': 5, 'y': 2}},
    {ID: 5, 'number': 15, 'color': 'red', 'dims': {'x': 3, 'y': 5}},
    {ID: 6, 'number': 15, 'color': 'blue', 'dims': {'x': 3, 'y': 5}},
    {ID: 7, 'number': 15, 'color': 'blue', 'dims': {'x': 5, 'y': 3}},
]
mk_kvs_versions('feature_cube')

# Sequence Annotations
# Demoing stream annotations with source and time interval
sequence_annots = [
    {'source': 'audio', 'bt': 5, 'tt': 7, 'annot': 'cat'},
    {
        'source': 'audio',
        'bt': 6,
        'tt': 9,
        'annot': 'dog',
        'comments': 'barks and chases cat away',
    },
    {'source': 'visual', 'bt': 5, 'tt': 8, 'annot': 'cat'},
    {
        'source': 'visual',
        'bt': 6,
        'tt': 15,
        'annot': 'dog',
        'comments': 'dog remains in view after bark ceases',
    },
]


####### Rock bands #####################################################################################################

rock_bands = [
    {
        '_id': 'pink_floyd_id',
        'name': 'Pink Floyd',
        'members': [
            {'_id': '1', 'firstname': 'Roger', 'lastname': 'Waters'},
            {'_id': '2', 'firstname': 'Nick', 'lastname': 'Mason'},
            {'_id': '3', 'firstname': 'Syd', 'lastname': 'Barrett'},
            {'_id': '4', 'firstname': 'Richard', 'lastname': 'Write'},
        ],
    },
    {
        '_id': 'the_doors_id',
        'name': 'The Doors',
        'members': [
            {'_id': '1', 'firstname': 'Jim', 'lastname': 'Morrison'},
            {'_id': '2', 'firstname': 'Ray', 'lastname': 'Manzarek'},
            {'_id': '3', 'firstname': 'Robby', 'lastname': 'Krieger'},
            {'_id': '4', 'firstname': 'John', 'lastname': 'Densmore'},
        ],
    },
]
mk_kvs_versions('rock_bands')


####### BDFLs ##########################################################################################################
# ubtained with:
# from scrapyng.tables import get_tables_from_url (if no scrapyng, use ut.webscrape.tables)
# t = get_tables_from_url('https://en.wikipedia.org/wiki/Benevolent_dictator_for_life')[0]
# t = t[['Name', 'Project', 'Type']].to_dict(orient='records')

bdfl = [
    {
        'Name': 'Sylvain Benner',
        'Project': 'Spacemacs',
        'Type': 'Community-driven Emacs distribution',
    },
    {
        'Name': 'Vitalik Buterin',
        'Project': 'Ethereum',
        'Type': 'Blockchain-based cryptocurrency',
    },
    {
        'Name': 'Dries Buytaert',
        'Project': 'Drupal',
        'Type': 'Content management framework',
    },
    {'Name': 'Haoyuan Li', 'Project': 'Alluxio', 'Type': 'Data Orchestration System',},
    {
        'Name': 'Evan Czaplicki',
        'Project': 'Elm',
        'Type': 'Front-end web programming language',
    },
    {
        'Name': 'David Heinemeier Hansson',
        'Project': 'Ruby on Rails',
        'Type': 'Web framework',
    },
    {'Name': 'Rich Hickey', 'Project': 'Clojure', 'Type': 'Programming language',},
    {
        'Name': 'Adrian Holovaty and Jacob Kaplan-Moss',
        'Project': 'Django',
        'Type': 'Web framework',
    },
    {
        'Name': 'Laurent Destailleur',
        'Project': 'Dolibarr ERP CRM',
        'Type': 'Software suite for Enterprise Resource Planning and Customer '
        'Relationship Management',
    },
    {
        'Name': 'Francois Chollet',
        'Project': 'Keras',
        'Type': 'Deep learning framework',
    },
    {'Name': 'Xavier Leroy', 'Project': 'OCaml', 'Type': 'Programming language',},
    {
        'Name': 'Yukihiro Matsumoto (Matz)',
        'Project': 'Ruby',
        'Type': 'Programming language',
    },
    {
        'Name': 'Wes McKinney',
        'Project': 'Pandas',
        'Type': 'Python data analysis library',
    },
    {'Name': 'Bram Moolenaar', 'Project': 'Vim', 'Type': 'Text editor'},
    {
        'Name': 'Matt Mullenweg [a]',
        'Project': 'WordPress',
        'Type': 'Content management framework',
    },
    {'Name': 'Martin Odersky', 'Project': 'Scala', 'Type': 'Programming language',},
    {'Name': 'Taylor Otwell', 'Project': 'Laravel', 'Type': 'Web framework'},
    {
        'Name': 'Theo de Raadt',
        'Project': 'OpenBSD',
        'Type': 'A Unix-like operating system',
    },
    {
        'Name': 'Ton Roosendaal[b]',
        'Project': 'Blender',
        'Type': '3D computer graphics software',
    },
    {
        'Name': 'Sébastien Ros',
        'Project': 'Orchard Project',
        'Type': 'Content management system',
    },
    {
        'Name': 'Mark Shuttleworth[c]',
        'Project': 'Ubuntu',
        'Type': 'Linux distribution',
    },
    {'Name': 'Don Syme[d]', 'Project': 'F#', 'Type': 'Programming language'},
    {
        'Name': 'Linus Torvalds[e]',
        'Project': 'Linux',
        'Type': 'Operating system kernel',
    },
    {'Name': 'José Valim', 'Project': 'Elixir', 'Type': 'Programming language',},
    {
        'Name': 'Pauli Virtanen',
        'Project': 'SciPy',
        'Type': 'Python library used for scientific and technical computing',
    },
    {
        'Name': 'Patrick Volkerding',
        'Project': 'Slackware',
        'Type': 'GNU/Linux distribution',
    },
    {
        'Name': 'Nathan Voxland',
        'Project': 'Liquibase',
        'Type': 'Database schema management',
    },
    {
        'Name': 'Shaun Walker',
        'Project': 'DotNetNuke',
        'Type': 'Web application framework',
    },
    {'Name': 'Larry Wall', 'Project': 'Perl', 'Type': 'Programming language'},
    {'Name': 'Jeremy Soller[37]', 'Project': 'Redox', 'Type': 'Operating system',},
    {
        'Name': 'Eugen Rochko',
        'Project': 'Mastodon',
        'Type': 'open source, decentralized social network',
    },
    {
        'Name': 'Dylan Araps',
        'Project': 'KISS Linux',
        'Type': 'a bare-bones Linux distribution based on musl libc and BusyBox',
    },
    {
        'Name': 'Gavin Mendel-Gleason[f]',
        'Project': 'TerminusDB',
        'Type': 'Open-source graph database for knowledge graph representation',
    },
]
bdfl_dict_kvs_3 = [
    ({ID: doc['Name']}, {'Project': doc['Project'], 'Type': doc['Type']})
    for doc in bdfl
]
bdfl_tuple_kvs_3 = [(k[ID], (v['Project'], v['Type'])) for k, v in bdfl_dict_kvs_3]

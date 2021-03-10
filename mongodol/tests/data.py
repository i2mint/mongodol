"""A module containing datas that will be used for tests"""

from mongodol.base import ID

# Schema not "perfect": Some missing fields here and there.
# Good to test no-sql cases
number_docs = [
    {ID: 1, 'en': 'one', 'fr': 'un', 'sp': 'uno', 'so_far': [1]},
    {ID: 2, 'en': 'two', 'fr': 'deux', 'so_far': [1, 2]},  # sp missing
    {ID: 3, 'en': 'three', 'fr': 'trois', 'sp': 'tres', 'so_far': [1, 2, 3]},
    {ID: 4, 'en': 'four', 'fr': 'quatre', 'sp': 'cuatro', 'so_far': [1, 2, 3, 4]},
    {ID: 5, 'en': 'five', 'sp': 'cinco', 'so_far': [1, 2, 3, 4, 5]},  # fr missing
]
number_dict_kvs_1 = [({ID: doc[ID]}, {k: doc[k] for k in doc.keys() - {ID}}) for doc in number_docs]
number_tuple_kvs_1 = [(k[ID], tuple(v.values())) for k, v in number_dict_kvs_1]

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
feature_cube_dict_kvs_1 = [({ID: doc[ID]}, {k: doc[k] for k in doc.keys() - {ID}}) for doc in feature_cube]
feature_cube_tuple_kvs_1 = [(k[ID], tuple(v.values())) for k, v in feature_cube_dict_kvs_1]

####### BDFLs ##########################################################################################################
# ubtained with:
# from scrapyng.tables import get_tables_from_url (if no scrapyng, use ut.webscrape.tables)
# t = get_tables_from_url('https://en.wikipedia.org/wiki/Benevolent_dictator_for_life')[0]
# t = t[['Name', 'Project', 'Type']].to_dict(orient='records')

bdfl_docs = [{'Name': 'Sylvain Benner',
              'Project': 'Spacemacs',
              'Type': 'Community-driven Emacs distribution'},
             {'Name': 'Vitalik Buterin',
              'Project': 'Ethereum',
              'Type': 'Blockchain-based cryptocurrency'},
             {'Name': 'Dries Buytaert',
              'Project': 'Drupal',
              'Type': 'Content management framework'},
             {'Name': 'Haoyuan Li',
              'Project': 'Alluxio',
              'Type': 'Data Orchestration System'},
             {'Name': 'Evan Czaplicki',
              'Project': 'Elm',
              'Type': 'Front-end web programming language'},
             {'Name': 'David Heinemeier Hansson',
              'Project': 'Ruby on Rails',
              'Type': 'Web framework'},
             {'Name': 'Rich Hickey', 'Project': 'Clojure', 'Type': 'Programming language'},
             {'Name': 'Adrian Holovaty and Jacob Kaplan-Moss',
              'Project': 'Django',
              'Type': 'Web framework'},
             {'Name': 'Laurent Destailleur',
              'Project': 'Dolibarr ERP CRM',
              'Type': 'Software suite for Enterprise Resource Planning and Customer '
                      'Relationship Management'},
             {'Name': 'Francois Chollet',
              'Project': 'Keras',
              'Type': 'Deep learning framework'},
             {'Name': 'Xavier Leroy', 'Project': 'OCaml', 'Type': 'Programming language'},
             {'Name': 'Yukihiro Matsumoto (Matz)',
              'Project': 'Ruby',
              'Type': 'Programming language'},
             {'Name': 'Wes McKinney',
              'Project': 'Pandas',
              'Type': 'Python data analysis library'},
             {'Name': 'Bram Moolenaar', 'Project': 'Vim', 'Type': 'Text editor'},
             {'Name': 'Matt Mullenweg [a]',
              'Project': 'WordPress',
              'Type': 'Content management framework'},
             {'Name': 'Martin Odersky', 'Project': 'Scala', 'Type': 'Programming language'},
             {'Name': 'Taylor Otwell', 'Project': 'Laravel', 'Type': 'Web framework'},
             {'Name': 'Theo de Raadt',
              'Project': 'OpenBSD',
              'Type': 'A Unix-like operating system'},
             {'Name': 'Ton Roosendaal[b]',
              'Project': 'Blender',
              'Type': '3D computer graphics software'},
             {'Name': 'Sébastien Ros',
              'Project': 'Orchard Project',
              'Type': 'Content management system'},
             {'Name': 'Mark Shuttleworth[c]',
              'Project': 'Ubuntu',
              'Type': 'Linux distribution'},
             {'Name': 'Don Syme[d]', 'Project': 'F#', 'Type': 'Programming language'},
             {'Name': 'Linus Torvalds[e]',
              'Project': 'Linux',
              'Type': 'Operating system kernel'},
             {'Name': 'José Valim', 'Project': 'Elixir', 'Type': 'Programming language'},
             {'Name': 'Pauli Virtanen',
              'Project': 'SciPy',
              'Type': 'Python library used for scientific and technical computing'},
             {'Name': 'Patrick Volkerding',
              'Project': 'Slackware',
              'Type': 'GNU/Linux distribution'},
             {'Name': 'Nathan Voxland',
              'Project': 'Liquibase',
              'Type': 'Database schema management'},
             {'Name': 'Shaun Walker',
              'Project': 'DotNetNuke',
              'Type': 'Web application framework'},
             {'Name': 'Larry Wall', 'Project': 'Perl', 'Type': 'Programming language'},
             {'Name': 'Jeremy Soller[37]', 'Project': 'Redox', 'Type': 'Operating system'},
             {'Name': 'Eugen Rochko',
              'Project': 'Mastodon',
              'Type': 'open source, decentralized social network'},
             {'Name': 'Dylan Araps',
              'Project': 'KISS Linux',
              'Type': 'a bare-bones Linux distribution based on musl libc and BusyBox'},
             {'Name': 'Gavin Mendel-Gleason[f]',
              'Project': 'TerminusDB',
              'Type': 'Open-source graph database for knowledge graph representation'}
             ]

bdfl_dict_kvs = [({ID: doc['Name']}, {'Project': doc['Project'], 'Type': doc['Type']}) for doc in bdfl_docs]
bdfl_tuple_kvs = [(k[ID], (v['Project'], v['Type'])) for k, v in bdfl_dict_kvs]

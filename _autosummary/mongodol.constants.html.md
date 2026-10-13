# mongodol.constants

Module to centralize constants used throughout project.

This includes enums, aliases, defaults, types, etc.

### Module Attributes

| [`DFLT_UUID_REPRESENTATION`](#mongodol.constants.DFLT_UUID_REPRESENTATION)   | The UUID codec of the clients mongodol makes.   |
|-----------------------------------------------------------------------------|-------------------------------------------------|

### mongodol.constants.DFLT_UUID_REPRESENTATION *= 'pythonLegacy'*

The UUID codec of the clients mongodol makes. pymongo 3 defaulted to the legacy
Python layout (BSON subtype 3); pymongo 4 defaults to “unspecified”, which cannot
encode a `uuid.UUID` and reads old ones back as `Binary`. Keeping the 3.x
layout means data written before the upgrade still reads back as `uuid.UUID`.

$env:OPENPLANT_TEST_HOST = "<host>"
$env:OPENPLANT_TEST_PORT = "8200"
$env:OPENPLANT_TEST_USER = "<user>"
$env:OPENPLANT_TEST_PASS = "<password>"
$env:OPENPLANT_TEST_READONLY = "1"

# Optional bounded target hints:
# $env:OPENPLANT_TEST_DB = "W3"
# $env:OPENPLANT_TEST_POINT_ID = "1001"
# $env:OPENPLANT_TEST_POINT_GN = "W3.NODE.POINT"

# Mutation examples/tests require explicit writable opt-in and isolated names.
# $env:OPENPLANT_TEST_MUTATION = "1"
# $env:OPENPLANT_TEST_READONLY = "0"
# $env:OPENPLANT_TEST_PREFIX = "SDK_MUTATION_"
# $env:OPENPLANT_TEST_APPLY_ADMIN = "1"
# $env:OPENPLANT_TEST_NODE_ID = "<isolated-node-id>"
# $env:OPENPLANT_TEST_SYSTEM_NODE_ID = "<isolated-system-node-id>"

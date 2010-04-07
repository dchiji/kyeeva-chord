-record(state, {
    myhash :: binary(),
    succlist :: list(tuple())
}).

-define(LENGTH_SUCCESSOR_LIST, 3).

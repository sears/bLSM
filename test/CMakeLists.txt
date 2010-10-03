IF( HAVE_STASIS )
  CREATE_CHECK(check_gen)
  CREATE_CHECK(check_bloomFilter)
  CREATE_CHECK(check_testAndSet)
  CREATE_CHECK(check_logtree)
  CREATE_CHECK(check_datapage)
  CREATE_CHECK(check_logtable)
  CREATE_CHECK(check_merge)
  CREATE_CHECK(check_mergelarge)
  CREATE_CHECK(check_mergetuple)
  CREATE_CHECK(check_rbtree)
  CREATE_CLIENT_EXECUTABLE(check_tcpclient)  # XXX should build this on non-stasis machines
  CREATE_CLIENT_EXECUTABLE(check_tcpbulkinsert)  # XXX should build this on non-stasis machines
ENDIF( HAVE_STASIS )

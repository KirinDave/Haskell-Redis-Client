module Test.GenericCommands where

import Test.Setup

tests = TList [TLabel "from*" test_unwrap_reply,
               TLabel "ping and renamed command" test_ping,
               TLabel "echo" test_echo,
               TLabel "exists and del" test_exists_del,
               TLabel "getType" test_get_type,
               TLabel "keys" test_keys,
               TLabel "randomKey" test_random_key,
               TLabel "rename" test_rename,
               TLabel "renameNx" test_renameNx,
               TLabel "dbsize" test_dbsize,
               TLabel "expire" test_expire,
               TLabel "ttl and persist" test_ttl_persist,
               TLabel "move" test_move,
               TLabel "flushDb" test_flushDb,
               TLabel "flushAll" test_flushAll]

test_unwrap_reply = TCase $ const $
                    let inline = return $ RInline "foo"
                        int = return $ RInt 1 :: IO (Reply String)
                        bulk = return $ RBulk $ Just "foo"                                          
                        mbulk = return $ RMulti $ Just [RBulk (Just "foo"), RBulk Nothing]
                        mbulk' = return $ RMulti Nothing :: IO (Reply String)
                        err = return $ RError "foo" :: IO (Reply String)
                    in do inline >>= fromRInline >>= assertEqual "" "foo"
                          int >>= fromRInt >>= assertEqual "" 1
                          bulk >>= fromRBulk >>= assertEqual "" (Just "foo")
                          mbulk >>= fromRMulti >>= assertEqual "" (Just [RBulk (Just "foo"), RBulk Nothing])
                          mbulk' >>= fromRMulti >>= assertEqual "" Nothing
                          mbulk >>= fromRMultiBulk >>= assertEqual "" (Just [Just "foo", Nothing])
                          mbulk' >>= fromRMultiBulk >>= assertEqual "" Nothing
                          mbulk >>= noError >>= assertEqual "" ()
                          assertRaises "" (ErrorCall undefined) (err >>= fromRInline)
                          assertRaises "" (ErrorCall undefined) (inline >>= fromRBulk)

test_ping = TCase $ testRedis $
    do r <- ask
       liftIO $ do renameCommand r (toBS "PING") (toBS "P") -- also tests "renamed command" feature
                   repl <- ping r
                   assertEqual "" RPong repl

test_echo = TCase $ testRedis $
    do r <- ask
       liftIO $ echo r "foobar" >>= fromRBulk >>= assertEqual "" (Just "foobar")

test_exists_del = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do e <- exists r "foo" >>= fromRInt
                   assertEqual "foo exists" 1 e
                   del r "foo"
                   e <- exists r "foo" >>= fromRInt
                   assertEqual "foo was deleted" 0 e

test_get_type = TCase $ testRedis $
    do addAll
       mapM_ (uncurry checkType) [("foo", RTString),
                                  ("bar", RTString),
                                  ("list", RTList),
                                  ("set", RTSet),
                                  ("zset", RTZSet),
                                  ("hash", RTHash),
                                  ("no-such-key", RTNone)]
    where checkType key t = do r <- ask
                               liftIO $ getType r key >>= assertEqual "" t

test_keys = TCase $ testRedis $
    do r <- ask
       addStr
       addSet
       addZSet
       liftIO $ do res <- keys r "nokey" >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   assertEqual "There's no key named \"nokey\"" (Just []) res
                   res <- keys r "foo" >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   assertEqual "One key named \"foo\"" (Just [Just "foo"]) res
                   res <- keys r "*set" >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   assertEqual "set and zset keys selected" (Just [Just "set", Just "zset"]) res

-- that seems randomKey always returns the same sequence of keys
test_random_key = TCase $ testRedis $
    do r <- ask
       addAll
       liftIO $ do res <- randomKey r >>= fromRBulk >>= return . fromJust
                   assertOneOf "" ["foo", "bar", "list", "set", "zset", "hash"] res

test_rename = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do exists r "zoo" >>= fromRInt >>= assertEqual "there is no zoo here!" 0
                   foo <- get r "foo" :: IO (Reply String)
                   rename r "foo" "zoo" >>= fromROk
                   exists r "foo" >>= fromRInt >>= assertEqual "foo was renamed to zoo" 0
                   exists r "zoo" >>= fromRInt >>= assertEqual "foo was renamed to zoo" 1
                   get r "zoo" >>= assertEqual "" foo

test_renameNx = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do renameNx r "foo" "bar" >>= fromRInt >>= assertEqual "" 0
                   exists r "foo" >>= fromRInt >>= assertEqual "" 1

test_dbsize = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do res <- dbsize r >>= fromRInt
                   real <- (keys r "*" :: IO (Reply String)) >>= fromRMultiBulk >>= return . length . fromJust
                   assertEqual "" real res

test_expire = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do expire r "foo" 0 >>= fromRInt >>= assertEqual "" 1
                   threadDelay 10
                   exists r "foo" >>= fromRInt >>= assertEqual "foo has to be expired now" 0
                   TOD now _ <- getClockTime
                   expireAt r "bar" $ fromIntegral now
                   threadDelay 10
                   exists r "bar" >>= fromRInt >>= assertEqual "bar has to be expired now" 0

test_ttl_persist = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do ttl r "foo" >>= fromRInt >>= assertEqual "" (-1)
                   expire r "foo" 30 >>= noError
                   ttl r "foo" >>= fromRInt >>= assertBool "TTL is greater then expiration timeout" . (<= 30)
                   persist r "foo" >>= noError
                   ttl r "foo" >>= fromRInt >>= assertEqual "foo was persisted again" (-1)

test_move = TCase $ \opts -> flip testRedis opts $
    do r <- ask
       addStr
       liftIO $ do exists r "foo" >>= fromRInt >>= assertEqual "foo has to be in first database" 1
                   move r "foo" $ (optDatabase opts) + 1
                   exists r "foo" >>= fromRInt >>= assertEqual "now foo is in second database" 0
                   select r $ (optDatabase opts) + 1
                   exists r "foo" >>= fromRInt >>= assertEqual "now foo is in second database" 1
                   move r "foo" $ optDatabase opts
                   select r $ optDatabase opts
                   return ()

test_flushDb = TCase $ testRedis $
    do r <- ask
       liftIO $ do select r 1
                   set r "leave_it_alone" "hooray!"
                   select r 0
                   flushDb r
                   dbsize r >>= fromRInt >>= assertEqual "first database was flushed" 0
                   select r 1
                   dbsize r >>= fromRInt >>= assertEqual "but not the second one" 1

test_flushAll = TCase $ testRedis $
    do r <- ask
       liftIO $ do select r 1
                   set r "baz" "no way"
                   select r 0
                   flushAll r
                   dbsize r >>= fromRInt >>= assertEqual "first database was flushed" 0
                   select r 1
                   dbsize r >>= fromRInt >>= assertEqual "and the second database too" 0

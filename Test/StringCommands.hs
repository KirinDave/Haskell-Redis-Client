module Test.StringCommands where

import Test.Setup
import qualified Data.ByteString as BS

tests = TList [TLabel "get and set" test_set_get,
               TLabel "setNx" test_setNx,
               TLabel "setEx" test_setEx,
               TLabel "mSet and mGet" test_m_set_get,
               TLabel "mSetNx" test_mSetNx,
               TLabel "getSet" test_getSet,
               TLabel "incr and incrBy, decr and decrBy" test_incr_decr,
               TLabel "append" test_append,
               TLabel "substr" test_substr,
               TLabel "getrange" test_getrange,
               TLabel "setrange" test_setrange,
               TLabel "getbit" test_getbit,
               TLabel "setbit" test_setbit,
               TLabel "strlen" test_strlen]

test_set_get = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do get r "foo" >>= fromRBulk >>= assertEqual "" (Just "foo")
                   set r "foo" "zoo" >>= fromROk
                   get r "foo" >>= fromRBulk >>= assertEqual "foo was set to zoo" (Just "zoo")

test_setNx = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do setNx r "foo" "zoo" >>= fromRInt >>= assertEqual "setNx doesn't replace key value" 0
                   get r "foo" >>= fromRBulk >>= assertEqual "" (Just "foo")

test_setEx = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do setEx r "foo" 30 "zoo" >>= fromROk
                   get r "foo" >>= fromRBulk >>= assertEqual "foo was set to zoo" (Just "zoo")
                   ttl r "foo" >>= fromRInt >>= assertBool "foo TTL must be less then 30 seconds" . (<= 30)

test_m_set_get = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do mSet r [("foo", "zoo"), ("zoo", "foo")] >>= fromROk
                   mGet r ["foo", "zoo", "baz"] >>= fromRMultiBulk >>= assertEqual "" (Just [Just "zoo", Just "foo", Nothing])

test_mSetNx = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do mSetNx r [("foo", "zoo"), ("zoo", "foo")] >>= fromRInt >>= assertEqual "foo already exists" 0
                   mGet r ["foo", "zoo"] >>= fromRMultiBulk >>= assertEqual "" (Just [Just "foo", Nothing])

test_getSet = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do getSet r "foo" "zoo" >>= fromRBulk >>= assertEqual "" (Just "foo")
                   get r "foo" >>= fromRBulk >>= assertEqual "foo was set to zoo" (Just "zoo")

test_incr_decr = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do set r "i" (0 :: Int) >>= fromROk
                   incr r "i" >>= fromRInt >>= assertEqual "" 1
                   (get r "i" :: IO (Reply String)) >>= fromRBulk >>= assertEqual "" (Just "1")
                   incrBy r "i" 2 >>= fromRInt >>= assertEqual "" 3
                   (get r "i" :: IO (Reply String)) >>= fromRBulk >>= assertEqual "" (Just "3")
                   decr r "i" >>= fromRInt >>= assertEqual "" 2
                   (get r "i" :: IO (Reply String)) >>= fromRBulk >>= assertEqual "" (Just "2")
                   decrBy r "i" 2 >>= fromRInt >>= assertEqual "" 0
                   (get r "i" :: IO (Reply String)) >>= fromRBulk >>= assertEqual "" (Just "0")

test_append = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do Just foo <- get r "foo" >>= fromRBulk
                   newlength <- append r "foo" "foo" >>= fromRInt
                   Just foo' <- get r "foo" >>= fromRBulk
                   assertEqual ("Expected: \"" ++ foo ++ "\" ++ \"foo\"") (foo ++ "foo") foo'
                   assertEqual "" (length foo') newlength

test_substr = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do Just foo <- get r "foo" >>= fromRBulk
                   let s = take 1 . drop 1 $ foo :: String
                   substr r "foo" (1, 1) >>= fromRBulk >>= assertEqual "" (Just s)

test_getrange = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do Just foo <- get r "foo" >>= fromRBulk
                   let s = take 1 . drop 1 $ foo :: String
                   getrange r "foo" (1, 1) >>= fromRBulk >>= assertEqual "" (Just s)

test_setrange = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do Just foo <- get r "foo" >>= fromRBulk
                   let s = (++ "bar") . take 2 $ foo :: String
                   setrange r "foo" 2 "bar" >>= fromRInt >>= assertEqual "" (length s)
                   get r "foo" >>= fromRBulk >>= assertEqual "" (Just s)

test_getbit = TCase $ testRedis $
    do r <- ask
       liftIO $ do (set r "foo" foo :: IO (Reply ())) >>= fromROk
                   getbit r "foo" 0 >>= fromRInt >>= assertEqual "" 0
                   getbit r "foo" 4 >>= fromRInt >>= assertEqual "" 1
                   getbit r "foo" 6 >>= fromRInt >>= assertEqual "" 1

    where foo = BS.pack [10] -- 0b00001010

test_setbit = TCase $ testRedis $
    do r <- ask
       liftIO $ do getbit r "foo" 0 >>= fromRInt >>= assertEqual "Non existing key" 0
                   setbit r "foo" 7 1 >>= fromRInt >>= assertEqual "Old value is 0" 0
                   setbit r "foo" 7 0 >>= fromRInt >>= assertEqual "Old value is 1" 1
                   setbit r "foo" 4 1 >>= fromRInt
                   setbit r "foo" 6 1 >>= fromRInt
                   get r "foo" >>= fromRBulk >>= assertEqual "" (Just foo)
    where foo = BS.pack [10] -- 0b00001010

test_strlen = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do Just foo <- get r "foo" >>= fromRBulk
                   strlen r "foo" >>= fromRInt >>= assertEqual ("lenght of \"" ++ foo ++ "\"" ) (length foo)

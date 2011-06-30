module Test.ListCommands where

import Prelude hiding ((!!))
import Data.List
import Test.Setup

tests = TList [TLabel "lrange" test_lrange,
               TLabel "llen" test_llen,
               TLabel "lpush and rpush" test_lpush_rpush,
               TLabel "lpushx and rpushx" test_lpushx_rpushx,
               TLabel "linsert" test_linsert,
               TLabel "ltrim" test_ltrim,
               TLabel "lindex and lset" test_lindex_lset,
               TLabel "lrem" test_lrem,
               TLabel "lpop and rpop" test_lpor_rpop,
               TLabel "rpoplpush" test_rpoplpush,
               TLabel "blpop" test_blpop,
               TLabel "brpop" test_brpop,
               TLabel "brpoplpush" test_brpoplpush]

test_lrange = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ do list <- lrange r "list" takeAll >>= fromRMultiBulk
                   assertEqual "" (Just [Just "1", Just "2", Just "3"]) list
                   list <- lrange r "list" (1, 1) >>= fromRMultiBulk
                   assertEqual "" (Just [Just "2"]) list

test_llen = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ do len <- llen r "list" >>= fromRInt
                   Just list <- lrange r "list" takeAll >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   assertEqual "equals to real list length" (length list) len
                   len <- llen r "no-such-key" >>= fromRInt
                   assertEqual "not existing key is the same as an empty list" 0 len

test_lpush_rpush = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ do Just list <- lrange r "list" takeAll >>= fromRMultiBulk
                   len <- rpush r "list" "4" >>= fromRInt
                   assertEqual "list length was increased by 1" (length list + 1) len
                   Just list' <- lrange r "list" takeAll >>= fromRMultiBulk
                   assertEqual "" (list ++ [(Just "4")]) list'

                   len <- lpush r "list" "0" >>= fromRInt
                   assertEqual "list length was increased by 1 again" (length list' + 1) len
                   Just list'' <- lrange r "list" takeAll >>= fromRMultiBulk
                   assertEqual "" ((Just "0") : list') list''

test_lpushx_rpushx = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ do Just list <- lrange r "list" takeAll >>= fromRMultiBulk
                   len <- rpushx r "list" "4" >>= fromRInt
                   assertEqual "list length was increased by 1" (length list + 1) len
                   Just list' <- lrange r "list" takeAll >>= fromRMultiBulk
                   assertEqual "" (list ++ [(Just "4")]) list'

                   len <- lpushx r "list" "0" >>= fromRInt
                   assertEqual "list length was increased by 1 again" (length list' + 1) len
                   Just list'' <- lrange r "list" takeAll >>= fromRMultiBulk
                   assertEqual "" ((Just "0") : list') list''

                   lpushx r "no-such-key" "1" >>= fromRInt >>= assertEqual "List was not created" 0
                   exists r "no-such-key" >>= fromRInt >>= assertEqual "List was not created" 0
                   rpushx r "no-such-key" "1" >>= fromRInt >>= assertEqual "List was not created" 0
                   exists r "no-such-key" >>= fromRInt >>= assertEqual "List was not created" 0

test_linsert = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ do Just list <- lrange r "list" takeAll >>= fromRMultiBulk
                   len <- linsert r "list" BEFORE "2" "foo" >>= fromRInt
                   assertEqual "list length was increased by 1" (length list + 1) len
                   Just list' <- lrange r "list" takeAll >>= fromRMultiBulk
                   let excepted = let (a,b) = span (/= Just "2") list
                                  in a ++ ((Just "foo") : b)
                   assertEqual "" excepted list'

                   len <- linsert r "list" AFTER "foo" "bar" >>= fromRInt
                   assertEqual "list length was increased by 1" (length list' + 1) len
                   Just list'' <- lrange r "list" takeAll >>= fromRMultiBulk
                   let excepted = let (a,b) = span (/= Just "foo") list'
                                  in a ++ (head b : Just "bar" : tail b)
                   assertEqual "" excepted list''

test_ltrim = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ do Just list <- lrange r "list" takeAll >>= fromRMultiBulk :: (IO (Maybe [Maybe String]))
                   ltrim r "list" (1, 1) >>= fromROk
                   Just list' <- lrange r "list" takeAll >>= fromRMultiBulk
                   assertEqual "" (take 1 $ drop 1 $ list) list'

test_lindex_lset = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ do Just list <- lrange r "list" takeAll >>= fromRMultiBulk :: (IO (Maybe [Maybe String]))
                   el <- lindex r "list" 1 >>= fromRBulk
                   assertEqual "" (list !! 1) el
                   lset r "list" 1 "4" >>= fromROk
                   lindex r "list" 1 >>= fromRBulk >>= assertEqual "" (Just "4")

test_lrem = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ do Just list <- lrange r "list" takeAll >>= fromRMultiBulk
                   lrem r "list" 1 (fromJust $ list !! 1) >>= fromRInt >>= assertEqual ("delete only first value equals to " ++ (fromJust $ list !! 1)) 1
                   Just list' <- lrange r "list" takeAll >>= fromRMultiBulk
                   assertEqual "" ((list !! 1) `delete` list) list'

test_lpor_rpop = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ do Just list <- lrange r "list" takeAll >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   lpop r "list" >>= fromRBulk >>= assertEqual "" (head list)
                   rpop r "list" >>= fromRBulk >>= assertEqual "" (last list)
                   Just list' <- lrange r "list" takeAll >>= fromRMultiBulk
                   assertEqual "" (init $ tail list) list'

test_rpoplpush = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ do Just list <- lrange r "list" takeAll >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   rpoplpush r "list" "list2" >>= fromRBulk >>= assertEqual "" (last list)
                   rpoplpush r "list" "list2" >>= fromRBulk >>= assertEqual "" (last $ init list)
                   Just list' <- lrange r "list" takeAll >>= fromRMultiBulk
                   assertEqual "Two elemens was rpopped" (init $ init list) list'
                   Just list'' <- lrange r "list2" takeAll >>= fromRMultiBulk
                   assertEqual "Two elemens was lpushed" (reverse $ take 2 $ reverse list) list''

test_blpop = TCase $ testRedis2 $
    do r1 <- ask
       r2 <- ask2
       addList
       liftIO $ do Just list <- lrange r1 "list" takeAll >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   res <- blpop r1 ["bar", "list"] 1
                   assertEqual "" (Just ("list", fromJust $ head list)) res
                   Just list' <- lrange r1 "list" takeAll >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   assertEqual "" (tail list) list'

                   later 500 $ lpush r2 "zap" "1" >>= noError
                   res <- blpop r1 ["zap"] 2
                   assertEqual "" (Just ("zap", "1")) res
                   exists r1 "zap" >>= fromRInt >>= assertEqual "" 0

                   (blpop r1 ["zap"] 1 :: IO (Maybe (String, String))) >>= assertEqual "" Nothing

test_brpop = TCase $ testRedis2 $
    do r1 <- ask
       r2 <- ask2
       addList
       liftIO $ do Just list <- lrange r1 "list" takeAll >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   res <- brpop r1 ["bar", "list"] 1
                   assertEqual "" (Just ("list", fromJust $ last list)) res
                   Just list' <- lrange r1 "list" takeAll >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   assertEqual "" (init list) list'

                   later 500 $ lpush r2 "zap" "1" >>= noError
                   res <- brpop r1 ["zap"] 2
                   assertEqual "" (Just ("zap", "1")) res
                   exists r1 "zap" >>= fromRInt >>= assertEqual "" 0

                   (brpop r1 ["zap"] 1 :: IO (Maybe (String, String))) >>= assertEqual "" Nothing

test_brpoplpush = TCase $ testRedis2 $
    do r1 <- ask
       r2 <- ask2
       addList
       liftIO $ do Just list <- lrange r1 "list" takeAll >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   res <- brpoplpush r1 "list" "bar" 0
                   assertEqual "" (Just $ Just $ fromJust $ last list) res
                   Just list' <- lrange r1 "list" takeAll >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   assertEqual "" (init list) list'
                   Just list' <- lrange r1 "bar" takeAll >>= fromRMultiBulk :: IO (Maybe [Maybe String])
                   assertEqual "" ([last list]) list'

                   later 500 $ lpush r2 "zap" "1" >>= noError
                   res <- brpoplpush r1 "zap" "list" 2
                   assertEqual "" (Just $ Just "1") res
                   exists r1 "zap" >>= fromRInt >>= assertEqual "" 0

                   (brpoplpush r1 "zap" "list" 1 :: IO (Maybe (Maybe String))) >>= assertEqual "" Nothing

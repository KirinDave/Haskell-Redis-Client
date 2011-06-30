module Test.SetCommands where

import Data.Set
import Test.Setup

tests = TList [TLabel "smembers" test_smembers,
               TLabel "sismember" test_sismember,
               TLabel "sadd" test_sadd,
               TLabel "srem" test_srem,
               TLabel "spop" test_spop,
               TLabel "smove" test_smove,
               TLabel "scard" test_scard,
               TLabel "srandmember" test_srandmember,
               TLabel "sinter and sinterStore" test_sinter_sinterstore,
               TLabel "sunion and sunionStore" test_sunion_sunionstore,
               TLabel "sdiff and sdiffStore" test_sdiff_sdiffstore]

asSet :: Reply String -> IO (Set (Maybe String))
asSet r = fromRMultiBulk r >>= return . fromList . fromJust
        
test_smembers = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do s <- smembers r "set" >>= asSet
                   assertEqual "" (fromList [Just "1", Just "2", Just "3"]) s

test_sismember = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do sismember r "set" "1" >>= fromRInt >>= assertEqual "" 1
                   sismember r "set" "0" >>= fromRInt >>= assertEqual "" 0
                   sismember r "no-such-key" "1" >>= fromRInt >>= assertEqual "" 0

test_sadd = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do s <- smembers r "set" >>= asSet
                   sadd r "set" "3" >>= fromRInt >>= assertEqual "element allready exists" 0
                   sadd r "set" "4" >>= fromRInt >>= assertEqual "" 1
                   s' <- smembers r "set" >>= asSet
                   assertEqual "" (insert (Just "4") s) s'

test_srem = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do s <- smembers r "set" >>= asSet
                   srem r "set" "4" >>= fromRInt >>= assertEqual "element is not a member" 0
                   srem r "set" "3" >>= fromRInt >>= assertEqual "" 1
                   s' <- smembers r "set" >>= asSet
                   assertEqual "" (delete (Just "3") s) s'

test_spop = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do s <- smembers r "set" >>= asSet
                   el <- spop r "set" >>= fromRBulk :: IO (Maybe String)
                   assertBool "" $ member el s
                   s' <- smembers r "set" >>= asSet
                   assertEqual "" (delete el s) s'

test_smove = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do s <- smembers r "set" >>= asSet
                   smove r "set" "set2" "3" >>= fromRInt >>= assertEqual "" 1
                   s' <- smembers r "set" >>= asSet
                   assertEqual "" (delete (Just "3") s) s'
                   s' <- smembers r "set2" >>= asSet
                   assertEqual "" (fromList [Just "3"]) s'

test_scard = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do s <- smembers r "set" >>= asSet
                   scard r "set" >>= fromRInt >>= assertEqual "" (size s)

test_srandmember = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do s <- smembers r "set" >>= asSet
                   el <- srandmember r "set" >>= fromRBulk
                   assertBool "" $ member el s

test_sinter_sinterstore = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do mapM_ (sadd r "set2") ["2", "3", "4"]
                   s <- smembers r "set" >>= asSet
                   s2 <- smembers r "set2" >>= asSet
                   s3 <- sinter r ["set", "set2"] >>= asSet
                   assertEqual "" (intersection s s2) s3
                   sinterStore r "set3" ["set", "set2"] >>= fromRInt >>= assertEqual "" (size s3)
                   smembers r "set3" >>= asSet >>= assertEqual "" s3
                   sinterStore r "set3" ["set", "set4"] >>= fromRInt >>= assertEqual "" 0

test_sunion_sunionstore = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do mapM_ (sadd r "set2") ["3", "4", "5"]
                   s <- smembers r "set" >>= asSet
                   s2 <- smembers r "set2" >>= asSet
                   s3 <- sunion r ["set", "set2"] >>= asSet
                   assertEqual "" (union s s2) s3
                   sunionStore r "set3" ["set", "set2"] >>= fromRInt >>= assertEqual "" (size s3)
                   smembers r "set3" >>= asSet >>= assertEqual "" s3
                   sunionStore r "set3" ["set", "set4"] >>= fromRInt >>= assertEqual "" (size s)

test_sdiff_sdiffstore = TCase $ testRedis $
    do r <- ask
       addSet
       liftIO $ do mapM_ (sadd r "set2") ["3", "4", "5"]
                   s <- smembers r "set" >>= asSet
                   s2 <- smembers r "set2" >>= asSet
                   s3 <- sdiff r ["set", "set2"] >>= asSet
                   assertEqual "" (difference s s2) s3
                   sdiffStore r "set3" ["set", "set2"] >>= fromRInt >>= assertEqual "" (size s3)
                   smembers r "set3" >>= asSet >>= assertEqual "" s3
                   sdiffStore r "set3" ["set", "set4"] >>= fromRInt >>= assertEqual "" (size s)

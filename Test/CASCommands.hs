module Test.CASCommands where

import Test.Setup

tests = TList [TLabel "watch/exec successful" test_watch_success,
               TLabel "wath/exec fail" test_watch_fail,
               TLabel "run_cas" test_run_cas,
               TLabel "run_cas that raised an exception" test_run_cas_exception]

test_watch_success = TCase $ testRedis $
    do r <- ask
       addStr
       liftIO $ do watch r ["foo", "bar"] >>= noError
                   Just foo <- get r "foo" >>= fromRBulk
                   multi r >>= noError
                   set r "foo" "bar" >>= noError
                   set r "foo_" foo >>= noError
                   exec r :: IO (Reply ())
                   foo_ <- get r "foo_" >>= fromRBulk :: IO (Maybe String)
                   assertEqual "" (Just foo) foo_
                   get r "foo" >>= fromRBulk >>= assertEqual "" (Just "bar")

test_watch_fail = TCase $ testRedis2 $
    do r1 <- ask
       r2 <- ask2
       addStr
       liftIO $ do watch r1 ["foo", "bar"] >>= noError
                   Just foo <- get r1 "foo" >>= fromRBulk :: IO (Maybe String)
                   multi r1 >>= noError
                   set r2 "foo" "baz" >>= fromROk
                   set r1 "foo" "bar" >>= noError -- note that this statement will not executed
                   set r1 "foo_" foo >>= noError
                   exec r1 :: IO (Reply ())
                   foo_ <- get r1 "foo_" >>= fromRBulk :: IO (Maybe String)
                   assertEqual "" Nothing foo_
                   get r1 "foo" >>= fromRBulk >>= assertEqual "" (Just "baz")

test_run_cas = TCase $ testRedis2 $
    let act :: Redis -> IO String
        act r = do Just foo <- get r "foo" >>= fromRBulk
                   multi r >>= noError
                   threadDelay 1000
                   set r "foo" "bar" >>= noError
                   set r "foo_" foo >>= noError
                   exec r :: IO (Reply ())
                   return foo

    in do r1 <- ask
          r2 <- ask2
          addStr
          liftIO $ do foo <- run_cas r1 ["foo", "bar"] act
                      foo_ <- get r1 "foo_" >>= fromRBulk :: IO (Maybe String)
                      assertEqual "" (Just foo) foo_
                      get r1 "foo" >>= fromRBulk >>= assertEqual "" (Just "bar")

                      del r1 "foo_"
                      later 500 $ set r2 "foo" "baz"
                      run_cas r1 ["foo", "bar"] act
                      foo_ <- get r1 "foo_" >>= fromRBulk :: IO (Maybe String)
                      assertEqual "" Nothing foo_
                      get r1 "foo" >>= fromRBulk >>= assertEqual "" (Just "baz")

test_run_cas_exception = TCase $ testRedis $
    let act :: Redis -> IO ()
        act r = do multi r >>= noError
                   set r "foo" "bar" >>= noError
                   set r "bar" "foo" >>= noError
                   error "bang!"
                   exec r :: IO (Reply ())
                   return ()
    in do r <- ask
          addStr
          liftIO $ do foo <- get r "foo" :: IO (Reply String)
                      assertRaises "" (ErrorCall undefined) $ run_cas r ["foo", "bar"] act
                      get r "foo" >>= assertEqual "" foo

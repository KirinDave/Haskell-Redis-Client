module Test.MultiCommands where

import Test.Setup

tests = TList [TLabel "multi and exec" test_multi_exec,
               TLabel "multi and discard" test_multi_discard,
               TLabel "run_multi" test_run_multi,
               TLabel "run_multi with exception" test_run_multi_exception]

test_multi_exec = TCase $ testRedis2 $
    do r1 <- ask
       r2 <- ask2
       addStr
       liftIO $ do foo <- get r2 "foo" >>= fromRBulk :: IO (Maybe String)
                   exists r2 "baz" >>= fromRInt >>= assertEqual "" 0

                   multi r1
                   set r1 "foo" "cool" >>= assertRQueued ""
                   set r1 "baz" "baz" >>= assertRQueued ""

                   get r2 "foo" >>= fromRBulk >>= assertEqual "exec was not called yet" foo
                   exists r2 "baz" >>= fromRInt >>= assertEqual "exec was not called yet" 0

                   (exec r1 :: IO (Reply String)) >>= fromRMulti >>= assertEqual "" (Just [ROk, ROk])

                   get r2 "foo" >>= fromRBulk >>= assertEqual "now foo got a new value" (Just "cool")
                   exists r2 "baz" >>= fromRInt >>= assertEqual "now baz is exists" 1

test_multi_discard = TCase $ testRedis2 $
    do r1 <- ask
       r2 <- ask2
       addStr
       liftIO $ do foo <- get r2 "foo" >>= fromRBulk :: IO (Maybe String)
                   exists r2 "baz" >>= fromRInt >>= assertEqual "" 0

                   multi r1
                   set r1 "foo" "cool" >>= assertRQueued ""
                   set r1 "baz" "baz" >>= assertRQueued ""

                   get r2 "foo" >>= fromRBulk >>= assertEqual "exec was not called yet" foo
                   exists r2 "baz" >>= fromRInt >>= assertEqual "exec was not called yet" 0

                   discard r1 >>= assertEqual "" ROk

                   get r2 "foo" >>= fromRBulk >>= assertEqual "statements was discarder" foo
                   exists r2 "baz" >>= fromRInt >>= assertEqual "statements was discarded" 0

test_run_multi = TCase $ testRedis2 $
    let act r = do (set r "foo" "cool" :: IO (Reply ())) >>= assertRQueued ""
                   (set r "baz" "baz" :: IO (Reply ())) >>= assertRQueued ""
                   threadDelay 2000

    in do r1 <- ask
          r2 <- ask2
          addStr
          liftIO $ do foo <- get r2 "foo" >>= fromRBulk :: IO (Maybe String)
                      exists r2 "baz" >>= fromRInt >>= assertEqual "" 0

                      v <- later 1000 $ get r2 "foo" >>= fromRBulk

                      run_multi r1 act  :: IO (Reply ())

                      takeMVar v >>= assertEqual "exec was not called yet" foo

                      get r2 "foo" >>= fromRBulk >>= assertEqual "now foo got a new value" (Just "cool")
                      exists r2 "baz" >>= fromRInt >>= assertEqual "now baz is exists" 1

test_run_multi_exception = TCase $ testRedis2 $
    let act r = do (set r "foo" "cool" :: IO (Reply ())) >>= assertRQueued ""
                   (set r "baz" "baz" :: IO (Reply ())) >>= assertRQueued ""
                   threadDelay 2000
                   error "bang!"

    in do r1 <- ask
          r2 <- ask2
          addStr
          liftIO $ do foo <- get r2 "foo" >>= fromRBulk :: IO (Maybe String)
                      exists r2 "baz" >>= fromRInt >>= assertEqual "" 0

                      v <- later 1000 $ get r2 "foo" >>= fromRBulk

                      assertRaises "" (ErrorCall undefined) $ (run_multi r1 act  :: IO (Reply ())) >> return ()

                      takeMVar v >>= assertEqual "exec was not called yet" foo

                      get r2 "foo" >>= fromRBulk >>= assertEqual "exception catched and multi statement was discarder" foo
                      exists r2 "baz" >>= fromRInt >>= assertEqual "exception catched and multi statement was discarder" 0

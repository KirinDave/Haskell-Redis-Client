{-# LANGUAGE ScopedTypeVariables #-}
module Test.Monad.MultiCommands where

import Database.Redis.Monad
import Test.Setup hiding (exec, del, exists, get, multi, run_multi, set)

tests = TList [TLabel "run_multi" test_run_multi,
               TLabel "run_multi with exception" test_run_multi_exception]

run r = liftIO . runWithRedis r

test_run_multi = TCase $ testRedis2 $
    let act = do set "foo" "cool" >>= liftIO . assertRQueued ""
                 set "baz" "baz" >>= liftIO . assertRQueued ""
                 liftIO $ threadDelay 2000

    in do r1 <- ask
          r2 <- ask2
          addStr
          (foo :: Maybe String) <- run r2 $ do foo <- get "foo" >>= fromRBulk
                                               exists "baz" >>= fromRInt >>= liftIO . assertEqual "" 0
                                               return foo

          v <- liftIO $ later 1000 $ runWithRedis r2 $ get "foo" >>= liftIO . fromRBulk

          run r1 $ (run_multi act :: RedisM (Reply ()))

          liftIO $ takeMVar v >>= assertEqual "exec was not called yet" foo

          run r2 $ do get "foo" >>= fromRBulk >>= liftIO . assertEqual "now foo got a new value" (Just "cool")
                      exists "baz" >>= fromRInt >>= liftIO . assertEqual "now baz is exists" 1

test_run_multi_exception = TCase $ testRedis2 $
    let act = do set "foo" "cool" >>= liftIO . assertRQueued ""
                 set "baz" "baz" >>= liftIO . assertRQueued ""
                 liftIO $ threadDelay 2000
                 error "bang!"

    in do r1 <- ask
          r2 <- ask2
          addStr
          foo <- run r2 $ do foo <- get "foo" >>= fromRBulk :: RedisM (Maybe String)
                             exists "baz" >>= fromRInt >>= liftIO . assertEqual "" 0
                             return foo

          v <- liftIO $ later 1000 $ runWithRedis r2 $ get "foo" >>= fromRBulk

          liftIO $ assertRaises "" (ErrorCall undefined) $ runWithRedis r1 $ (run_multi act :: RedisM (Reply ())) >> return ()

          liftIO $ takeMVar v >>= assertEqual "exec was not called yet" foo

          run r2 $ do get "foo" >>= fromRBulk >>= liftIO . assertEqual "exception catched and multi statement was discarder" foo
                      exists "baz" >>= fromRInt >>= liftIO . assertEqual "exception catched and multi statement was discarder" 0
